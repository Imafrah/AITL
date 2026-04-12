import csv
import io
import re
import logging

import pandas as pd

from parsers.txt_parser import ParseError
from utils.data_cleaner import clean_csv_row, clean_csv_text_output, get_cleaning_stats

logger = logging.getLogger("csv_parser")


def normalize_field_name(s: str | None) -> str:
    """Lowercase, trim, strip BOM, collapse spaces/underscores for header matching."""
    if s is None:
        return ""
    t = str(s).strip().lower().replace("\ufeff", "")
    t = re.sub(r"[\s_\-]+", "_", t)
    return t.strip("_")


# --- Canonical keys expected by process_*_row (original casing) ---

_TRANSACTION_ALIASES: dict[str, str] = {
    "transaction_id": "Transaction_ID",
    "trans_id": "Transaction_ID",
    "txn_id": "Transaction_ID",
    "txn": "Transaction_ID",
    "order_id": "Transaction_ID",
    "invoice_id": "Transaction_ID",
    "customer_id": "Customer_ID",
    "customer": "Customer_ID",
    "client_id": "Customer_ID",
    "client": "Customer_ID",
    "buyer": "Customer_ID",
    "customer_name": "Customer_ID",
    "payer": "Customer_ID",
    "payment_method": "Payment_Method",
    "pay_method": "Payment_Method",
    "payment_type": "Payment_Method",
    "method": "Payment_Method",
    "pay_mode": "Payment_Method",
    "price": "Price",
    "amount": "Price",
    "total": "Price",
    "value": "Price",
    "cost": "Price",
    "payment": "Price",
    "grand_total": "Price",
    "transaction_date": "Transaction_Date",
    "date": "Transaction_Date",
    "paid_date": "Transaction_Date",
    "order_date": "Transaction_Date",
    "transaction_status": "Transaction_Status",
    "status": "Transaction_Status",
    "state": "Transaction_Status",
}

_EMPLOYEE_ALIASES: dict[str, str] = {
    "employee_id": "Employee_ID",
    "emp_id": "Employee_ID",
    "emp_no": "Employee_ID",
    "staff_id": "Employee_ID",
    "name": "Name",
    "full_name": "Name",
    "employee_name": "Name",
    "emp_name": "Name",
    "first_name": "Name",
    "department": "Department",
    "dept": "Department",
    "division": "Department",
    "team": "Department",
    "salary": "Salary",
    "wage": "Salary",
    "pay": "Salary",
    "compensation": "Salary",
    "annual_salary": "Salary",
    "base_salary": "Salary",
}

_SALES_ALIASES: dict[str, str] = {
    "vendor": "vendor",
    "supplier": "vendor",
    "merchant": "vendor",
    "seller": "vendor",
    "amount": "amount",
    "total": "amount",
    "price": "amount",
    "value": "amount",
    "currency": "currency",
    "curr": "currency",
    "date": "date",
    "sale_date": "date",
    "invoice_date": "date",
}


def _apply_aliases(row: dict, alias_map: dict[str, str]) -> dict:
    """Copy row and fill canonical keys from any column whose normalized name matches."""
    out = dict(row)
    for orig_k, v in row.items():
        nk = normalize_field_name(orig_k)
        canon = alias_map.get(nk)
        if canon is None:
            continue
        if v in (None, ""):
            continue
        if out.get(canon) in (None, ""):
            out[canon] = v
    return out


def _detect_csv_schema(fieldnames: list[str]) -> str:
    """
    Pick transaction | employee | sales | generic from headers (case/spacing tolerant).
    Order matters: transaction before employee to avoid invoice+total false positives.
    """
    norms = {normalize_field_name(f) for f in fieldnames if f is not None and str(f).strip()}

    def has_any(keys: set[str]) -> bool:
        return bool(norms & keys)

    name_h = {"name", "full_name", "employee_name", "emp_name", "first_name", "last_name"}
    dept_h = {"department", "dept", "division", "team"}
    sal_h = {"salary", "wage", "compensation", "annual_salary", "base_salary"}
    emp_id_h = {"employee_id", "emp_id", "emp_no", "staff_id"}
    txn_h = {
        "transaction_id",
        "trans_id",
        "txn_id",
        "txn",
        "order_id",
        "invoice_id",
    }
    pay_h = {"payment_method", "pay_method", "payment_type", "pay_mode"}
    # "method" alone is weak; "payment" as column often means amount column name
    price_h = {"price", "amount", "total", "value", "cost", "grand_total", "subtotal", "payment"}
    cust_h = {"customer_id", "customer", "client_id", "client", "buyer", "payer", "customer_name"}
    ven_h = {"vendor", "supplier", "merchant", "seller"}

    # Transactions / invoices
    if (has_any(pay_h) and has_any(price_h)) or (
        has_any(txn_h) and (has_any(pay_h) or has_any(price_h))
    ):
        return "transaction"
    if has_any(cust_h) and has_any(price_h):
        return "transaction"

    # HR-style rows (avoid matching on generic "pay" — use compensation keywords)
    sal_strict = {"salary", "wage", "compensation", "annual_salary", "base_salary"}
    if (has_any(name_h) or has_any(emp_id_h)) and (has_any(dept_h) or has_any(sal_strict)):
        return "employee"

    # Sales extract
    if has_any(ven_h) and has_any(price_h):
        return "sales"

    return "generic"


_AMOUNT_HINTS = (
    "price",
    "amount",
    "total",
    "salary",
    "wage",
    "cost",
    "fee",
    "qty",
    "quantity",
    "balance",
    "revenue",
    "tax",
    "discount",
    "subtotal",
    "msrp",
    "rrp",
    "rate",
    "unit_price",
    "net",
    "gross",
)

_SKIP_AMOUNT_COL = frozenset({"id", "index", "row", "line", "version", "year", "month", "zip", "pin"})


def process_generic_tabular_row(row: dict, row_index: int) -> dict:
    """Best-effort extraction for arbitrary CSV columns (no fixed schema)."""
    parts = [str(v).strip() for v in list(row.values())[:3] if v not in (None, "")]
    slug = re.sub(r"[^\w\-]+", "_", "_".join(parts))[:56].strip("_") or f"r{row_index}"
    doc_id = f"row-{slug}"

    entities: dict = {"person_names": [], "organizations": [], "dates": [], "amounts": []}
    extra_fields: dict = {}
    pid = oid = did = aid = 0

    for orig_k, v in row.items():
        if v is None:
            continue
        sval = str(v).strip()
        if not sval:
            continue
        nk = normalize_field_name(orig_k)

        num = safe_float(sval)
        if num is not None and nk not in _SKIP_AMOUNT_COL:
            if any(h in nk for h in _AMOUNT_HINTS):
                aid += 1
                entities["amounts"].append(
                    {
                        "id": f"a{aid}",
                        "value": num,
                        "currency": "USD",
                        "label": nk or "amount",
                        "confidence": 0.82,
                    }
                )
                continue

        if any(x in nk for x in ("date", "time", "timestamp")) and len(sval) >= 6:
            did += 1
            entities["dates"].append({"id": f"d{did}", "value": sval, "confidence": 0.72})
            continue

        if any(
            x in nk
            for x in (
                "name",
                "person",
                "user",
                "customer",
                "client",
                "employee",
                "owner",
                "author",
            )
        ):
            pid += 1
            entities["person_names"].append({"id": f"p{pid}", "value": sval, "confidence": 0.78})
            continue

        if any(
            x in nk
            for x in (
                "org",
                "company",
                "vendor",
                "dept",
                "department",
                "division",
                "team",
                "merchant",
                "supplier",
                "store",
                "brand",
                "city",
                "country",
                "region",
            )
        ):
            oid += 1
            entities["organizations"].append({"id": f"o{oid}", "value": sval, "confidence": 0.74})
            continue

        extra_fields[str(orig_k)] = v

    has_entities = any(entities[k] for k in entities)
    status = "success" if (has_entities or extra_fields) else "partial"
    err = None if status == "success" else "No extractable fields; see metadata.extra_fields for raw cells."

    meta = {"file_type": "csv", "columns": list(row.keys())}
    if extra_fields:
        meta["extra_fields"] = extra_fields

    return {
        "document_id": doc_id,
        "document_type": "generic_csv",
        "status": status,
        "error": err,
        "entities": entities,
        "relationships": [],
        "metadata": meta,
    }

def normalize_payment(method: str) -> str:
    """Normalize payment method strings."""
    if not method:
        return ""
    m = method.strip().lower()
    if m in ["paypal", "pay pal"]:
        return "PayPal"
    if m in ["creditcard", "credit card"]:
        return "Credit Card"
    if m == "cash":
        return "Cash"
    return method.strip().title()

def safe_float(value: str) -> float | None:
    """Safely convert string to float."""
    if not value:
        return None
    # Remove currency symbols and commas if any
    clean_val = str(value).replace('$', '').replace(',', '').strip()
    try:
        return float(clean_val)
    except ValueError:
        return None

def process_transaction_row(row: dict, row_index: int) -> dict:
    doc_id = row.get("Transaction_ID") or f"txn-{row_index}"
    
    # Extract data
    date_val = row.get("Transaction_Date", "")
    customer = row.get("Customer_ID", "")
    price = safe_float(row.get("Price", ""))
    payment = normalize_payment(row.get("Payment_Method", ""))
    status = str(row.get("Transaction_Status", "")).lower()
    
    entities = {
        "person_names": [],
        "organizations": [],
        "dates": [],
        "amounts": []
    }
    relationships = []
    
    p1_id = "p1"
    if customer:
        entities["person_names"].append({"id": p1_id, "value": customer, "confidence": 1.0})
        
    o1_id = "o1"
    if payment:
        entities["organizations"].append({"id": o1_id, "value": payment, "confidence": 0.95})
        
    d1_id = "d1"
    if date_val:
        entities["dates"].append({"id": d1_id, "value": date_val, "confidence": 1.0})
        
    a1_id = "a1"
    if price is not None:
        entities["amounts"].append({"id": a1_id, "value": price, "currency": "USD", "label": "transaction_amount", "confidence": 1.0})
        
    if customer and payment:
        rel_attrs = {}
        if price is not None: 
            rel_attrs["amount"] = a1_id
        if date_val: 
            rel_attrs["date"] = d1_id
            
        relationships.append({
            "type": "payment",
            "from": p1_id,
            "to": o1_id,
            "confidence": 0.95,
            "attributes": rel_attrs
        })

    doc_status = "success"
    if status and status != "success" and status != "completed":
        doc_status = "partial"

    return {
        "document_id": doc_id,
        "document_type": "transaction",
        "status": doc_status,
        "error": None,
        "entities": entities,
        "relationships": relationships,
        "metadata": {
            "file_type": "csv"
        }
    }

def process_employee_row(row: dict, row_index: int) -> dict:
    doc_id = row.get("Employee_ID") or f"emp-{row_index}"
    
    name = row.get("Name", "")
    dept = row.get("Department", "")
    salary = safe_float(row.get("Salary", ""))
    
    entities = {
        "person_names": [],
        "organizations": [],
        "dates": [],
        "amounts": []
    }
    relationships = []
    
    p1_id = "p1"
    if name:
        entities["person_names"].append({"id": p1_id, "value": name, "confidence": 1.0})
        
    o1_id = "o1"
    if dept:
        entities["organizations"].append({"id": o1_id, "value": dept, "confidence": 1.0})
        
    a1_id = "a1"
    if salary is not None:
        entities["amounts"].append({"id": a1_id, "value": salary, "currency": "USD", "label": "salary", "confidence": 1.0})
        
    if name and dept:
        rel_attrs = {}
        if salary is not None: 
            rel_attrs["amount"] = a1_id
            
        relationships.append({
            "type": "employed_by",
            "from": p1_id,
            "to": o1_id,
            "confidence": 1.0,
            "attributes": rel_attrs
        })

    return {
        "document_id": doc_id,
        "document_type": "employee_record",
        "status": "success",
        "error": None,
        "entities": entities,
        "relationships": relationships,
        "metadata": {
            "file_type": "csv"
        }
    }

def process_sales_row(row: dict, row_index: int) -> dict:
    doc_id = f"sales-{row_index}"
    vendor = row.get("vendor", "")
    amount = safe_float(row.get("amount", ""))
    currency = row.get("currency", "USD")
    date_val = row.get("date", "")
    
    entities = {"person_names": [], "organizations": [], "dates": [], "amounts": []}
    relationships = []
    
    o1_id = "o1"
    if vendor:
        entities["organizations"].append({"id": o1_id, "value": vendor, "confidence": 1.0})
        
    d1_id = "d1"
    if date_val:
        entities["dates"].append({"id": d1_id, "value": date_val, "confidence": 1.0})
        
    a1_id = "a1"
    if amount is not None:
        entities["amounts"].append({"id": a1_id, "value": amount, "currency": currency, "label": "sale_amount", "confidence": 1.0})
        
    if vendor and amount is not None:
        relationships.append({
            "type": "sale",
            "from": o1_id,
            "to": "unknown_buyer",
            "confidence": 0.9,
            "attributes": {"amount": a1_id}
        })
        
    return {
        "document_id": doc_id,
        "document_type": "sales_record",
        "status": "success",
        "error": None,
        "entities": entities,
        "relationships": relationships,
        "metadata": {"file_type": "csv"}
    }


def parse_csv(file_bytes: bytes) -> dict:
    """
    Build cleaned plain text + metadata from CSV for the AI pipeline (orchestrator).
    """
    try:
        try:
            text_io = io.StringIO(file_bytes.decode("utf-8-sig"))
        except UnicodeDecodeError:
            text_io = io.StringIO(file_bytes.decode("latin-1"))

        df = pd.read_csv(text_io)

        if df.empty:
            raise ParseError("CSV file is empty or has no data rows.")

        original_rows = df.to_dict(orient="records")
        cleaned_rows = [clean_csv_row(dict(row)) for row in original_rows]
        cleaned_df = pd.DataFrame(cleaned_rows)
        cleaned_df = cleaned_df.where(pd.notnull(cleaned_df), None)

        stats = get_cleaning_stats(original_rows, cleaned_rows)

        text = cleaned_df.to_string(index=False)
        text = clean_csv_text_output(text)

        if not text.strip():
            raise ParseError("File has no usable content after cleaning.")

        return {
            "text": text,
            "metadata": {
                "file_type": "csv",
                "page_count": None,
                "word_count": len(text.split()),
                "row_count": len(cleaned_df),
                "columns": list(cleaned_df.columns),
                "cleaning_stats": stats,
            },
        }
    except ParseError:
        raise
    except Exception as e:
        raise ParseError(f"Failed to parse CSV: {e}") from e


def parse_csv_documents(file_bytes: bytes) -> list[dict]:
    """Parse CSV into one structured document per row (translate API, no AI)."""
    try:
        content = file_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        content = file_bytes.decode("latin-1")

    reader = csv.DictReader(io.StringIO(content))
    raw_headers = reader.fieldnames or []
    fieldnames = [str(f).strip() for f in raw_headers if f is not None and str(f).strip()]
    schema = _detect_csv_schema(fieldnames)
    logger.info("CSV schema=%s | headers=%s", schema, fieldnames)

    results = []
    MAX_ROWS = 1000

    for idx, row in enumerate(reader):
        if idx >= MAX_ROWS:
            logger.warning(f"CSV exceeded maximum allowed rows ({MAX_ROWS}). Truncating.")
            break

        if not any(str(v).strip() for v in row.values() if v is not None):
            continue

        row = clean_csv_row(dict(row))

        try:
            if schema == "transaction":
                doc = process_transaction_row(_apply_aliases(row, _TRANSACTION_ALIASES), idx + 1)
            elif schema == "employee":
                doc = process_employee_row(_apply_aliases(row, _EMPLOYEE_ALIASES), idx + 1)
            elif schema == "sales":
                doc = process_sales_row(_apply_aliases(row, _SALES_ALIASES), idx + 1)
            else:
                doc = process_generic_tabular_row(row, idx + 1)
            results.append(doc)

        except Exception as e:
            logger.error(f"Error processing row {idx}: {e}")
            results.append(
                {
                    "document_id": f"error-{idx}",
                    "document_type": "error",
                    "status": "failed",
                    "error": str(e),
                    "entities": {
                        "person_names": [],
                        "organizations": [],
                        "dates": [],
                        "amounts": [],
                    },
                    "relationships": [],
                    "metadata": {"file_type": "csv"},
                }
            )

    return results