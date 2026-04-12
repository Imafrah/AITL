import csv
import io
import re
import logging
from typing import Any

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


def dynamic_map_row(row: dict, mapping: dict[str, Any]) -> dict[str, Any]:
    """
    Map CSV row values into semantic slots using AI/rule-provided column lists.
    Column names are matched with the same normalization as schema detection.
    """
    by_norm: dict[str, Any] = {}
    for k, v in row.items():
        nk = normalize_field_name(str(k))
        if nk:
            by_norm[nk] = v
    out: dict[str, Any] = {}
    for target, cols in mapping.items():
        if cols is None:
            continue
        if not isinstance(cols, list):
            cols = [cols]
        for col in cols:
            cn = normalize_field_name(str(col))
            if not cn:
                continue
            if cn in by_norm:
                val = by_norm[cn]
                if val not in (None, ""):
                    out[str(target)] = val
                    break
    return out


def _ai_row_document_id(row: dict, mapped: dict[str, Any], row_index: int) -> str:
    for k, v in row.items():
        nk = normalize_field_name(str(k))
        if nk.endswith("_id") or nk == "id":
            s = str(v).strip() if v is not None else ""
            if s:
                return s[:120]
    for v in mapped.values():
        if v not in (None, ""):
            slug = re.sub(r"[^\w\-]+", "_", str(v).strip())[:48]
            if slug:
                return f"ai-{row_index}-{slug}"
    return f"ai-{row_index}"


def _ai_mapped_relationships(schema_type: str, entities: dict) -> list[dict]:
    st = (schema_type or "generic").lower()
    rels: list[dict] = []
    has_p = bool(entities.get("person_names"))
    has_o = bool(entities.get("organizations"))
    has_a = bool(entities.get("amounts"))

    if st == "employee" and has_p and has_o:
        rel_attrs: dict = {}
        if has_a:
            rel_attrs["amount"] = "a1"
        rels.append(
            {
                "type": "employed_by",
                "from": "p1",
                "to": "o1",
                "confidence": 0.85,
                "attributes": rel_attrs,
            }
        )
    elif st in ("transaction", "invoice") and has_p and has_o:
        rel_attrs = {}
        if has_a:
            rel_attrs["amount"] = "a1"
        if entities.get("dates"):
            rel_attrs["date"] = "d1"
        rels.append(
            {
                "type": "payment",
                "from": "p1",
                "to": "o1",
                "confidence": 0.82,
                "attributes": rel_attrs,
            }
        )
    elif st == "sales" and has_o and has_a:
        rels.append(
            {
                "type": "sale",
                "from": "o1",
                "to": "unknown_buyer",
                "confidence": 0.8,
                "attributes": {"amount": "a1"},
            }
        )
    return rels


def process_ai_mapped_row(
    row: dict,
    mapping: dict[str, Any],
    schema_type: str,
    row_index: int,
) -> dict:
    """Build one structured document from a row using AI-derived column mapping."""
    m = dynamic_map_row(row, mapping)
    doc_id = _ai_row_document_id(row, m, row_index)

    entities: dict = {"person_names": [], "organizations": [], "dates": [], "amounts": []}

    pn = m.get("person_name")
    if pn not in (None, ""):
        entities["person_names"].append(
            {"id": "p1", "value": str(pn).strip(), "confidence": 0.88}
        )

    org = m.get("organization")
    if org not in (None, ""):
        entities["organizations"].append(
            {"id": "o1", "value": str(org).strip(), "confidence": 0.85}
        )

    dv = m.get("date")
    if dv not in (None, ""):
        entities["dates"].append(
            {"id": "d1", "value": str(dv).strip(), "confidence": 0.82}
        )

    amt = m.get("amount")
    if amt is not None:
        num = safe_float(str(amt))
        if num is not None:
            cur_raw = m.get("currency")
            if cur_raw is None or str(cur_raw).strip() == "":
                cur_s = "USD"
            else:
                cur_s = str(cur_raw).strip()[:8]
            entities["amounts"].append(
                {
                    "id": "a1",
                    "value": num,
                    "currency": cur_s,
                    "label": "amount",
                    "confidence": 0.86,
                }
            )

    relationships = _ai_mapped_relationships(schema_type, entities)

    doc_type_map = {
        "employee": "employee_record",
        "transaction": "transaction",
        "invoice": "transaction",
        "sales": "sales_record",
        "generic": "ai_mapped_csv",
    }
    document_type = doc_type_map.get((schema_type or "generic").lower(), "ai_mapped_csv")

    status = "success" if any(entities[k] for k in entities) else "partial"
    err = (
        None
        if status == "success"
        else "AI mapping produced no extractable fields for this row."
    )

    return {
        "document_id": doc_id,
        "document_type": document_type,
        "status": status,
        "error": err,
        "entities": entities,
        "relationships": relationships,
        "metadata": {
            "file_type": "csv",
            "schema_source": "ai",
            "ai_schema_type": schema_type,
        },
    }


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


def parse_csv_documents(file_bytes: bytes, api_key: str | None = None) -> list[dict]:
    """
    Parse CSV into one structured document per row (translate API).

    Hybrid: rule-based schema when confident; otherwise optional AI column mapping
    when ``api_key`` is set (same env as main extractor).
    """
    try:
        content = file_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        content = file_bytes.decode("latin-1")

    from parsers.csv_robust import CSVParsingError, parse_csv_text_to_rows

    try:
        fieldnames, rows = parse_csv_text_to_rows(content)
    except CSVParsingError as e:
        raise ParseError(str(e)) from e

    schema = _detect_csv_schema(fieldnames)
    logger.info("CSV schema=%s | headers=%s | rows=%s", schema, fieldnames, len(rows))

    ai_mapping: dict[str, Any] | None = None
    ai_schema_type = "generic"

    if schema == "generic" and api_key and str(api_key).strip() and rows:
        sample: list[dict] = []
        for raw in rows[:8]:
            if any(str(v).strip() for v in raw.values() if v is not None):
                sample.append(clean_csv_row(dict(raw)))
            if len(sample) >= 5:
                break
        if sample:
            try:
                from ai_layer.schema_detector import detect_schema_ai

                ai_result = detect_schema_ai(sample, api_key)
                raw_map = ai_result.get("mapping") or {}
                if raw_map:
                    ai_mapping = raw_map
                    ai_schema_type = str(ai_result.get("schema_type", "generic")).lower()
                    logger.info(
                        "CSV AI schema=%s | mapped_roles=%s",
                        ai_schema_type,
                        list(ai_mapping.keys()),
                    )
                else:
                    logger.warning(
                        "AI schema detection returned empty mapping; using heuristics."
                    )
            except Exception as e:
                logger.warning("AI schema detection skipped: %s", e)

    results: list[dict] = []
    MAX_ROWS = 1000

    for idx, raw_row in enumerate(rows):
        if idx >= MAX_ROWS:
            logger.warning("CSV exceeded maximum allowed rows (%s). Truncating.", MAX_ROWS)
            break

        if not any(str(v).strip() for v in raw_row.values() if v is not None):
            continue

        row = clean_csv_row(dict(raw_row))

        try:
            if schema == "transaction":
                doc = process_transaction_row(
                    _apply_aliases(row, _TRANSACTION_ALIASES), idx + 1
                )
            elif schema == "employee":
                doc = process_employee_row(_apply_aliases(row, _EMPLOYEE_ALIASES), idx + 1)
            elif schema == "sales":
                doc = process_sales_row(_apply_aliases(row, _SALES_ALIASES), idx + 1)
            elif ai_mapping:
                doc = process_ai_mapped_row(row, ai_mapping, ai_schema_type, idx + 1)
            else:
                doc = process_generic_tabular_row(row, idx + 1)
            results.append(doc)

        except Exception as e:
            logger.error("Error processing row %s: %s", idx, e)
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