import csv
import io
import uuid
import logging

import pandas as pd

from parsers.txt_parser import ParseError
from utils.data_cleaner import clean_csv_row, clean_csv_text_output, get_cleaning_stats

logger = logging.getLogger("csv_parser")

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
    fieldnames = reader.fieldnames or []

    is_transaction = any(
        k in fieldnames
        for k in ["Transaction_ID", "Payment_Method", "Transaction_Status", "Customer_ID"]
    )
    is_employee = any(k in fieldnames for k in ["Employee_ID", "Department", "Salary", "Name"])
    is_sales = any(k in fieldnames for k in ["vendor", "amount"])

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
            if is_transaction:
                doc = process_transaction_row(row, idx + 1)
            elif is_employee:
                doc = process_employee_row(row, idx + 1)
            elif is_sales:
                doc = process_sales_row(row, idx + 1)
            else:
                doc = {
                    "document_id": f"row-{uuid.uuid4()}",
                    "document_type": "unknown_csv",
                    "status": "partial",
                    "error": "Unrecognized CSV schema format.",
                    "entities": {
                        "person_names": [],
                        "organizations": [],
                        "dates": [],
                        "amounts": [],
                    },
                    "relationships": [],
                    "metadata": {"file_type": "csv"},
                }
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