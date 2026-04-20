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


def safe_float(value: str) -> float | None:
    """Safely convert string to float."""
    if not value:
        return None
    # Remove currency symbols and commas if any
    clean_val = str(value).replace('$', '').replace(',', '').strip()
    # Remove other common currency symbols
    clean_val = re.sub(r'^[\s€£₹¥₩₽]+', '', clean_val)
    try:
        return float(clean_val)
    except ValueError:
        return None


def normalize_payment(method: str) -> str:
    """Normalize payment method strings (pattern-based, not hardcoded list)."""
    if not method:
        return ""
    return method.strip().title()


def process_generic_tabular_row(row: dict, row_index: int) -> dict:
    """
    Universal extraction for ANY CSV columns — no fixed schema assumptions.
    All field classification done by value patterns, not column name keywords.
    """
    from core.data_profiler import (
        _looks_like_email,
        _looks_like_phone,
        _looks_like_date,
        _coerce_number,
    )

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

        # Classify by VALUE PATTERN, not column name
        num = safe_float(sval)

        # Email pattern detection
        if _looks_like_email(sval):
            extra_fields[str(orig_k)] = sval.lower()
            continue

        # Phone pattern detection
        if _looks_like_phone(sval):
            extra_fields[str(orig_k)] = sval
            continue

        # Date pattern detection
        if len(sval) >= 6 and _looks_like_date(sval):
            did += 1
            entities["dates"].append({"id": f"d{did}", "value": sval, "confidence": 0.72})
            continue

        # Numeric / monetary values
        if num is not None:
            # Large numbers or decimals are likely amounts
            if abs(num) >= 100 or (num != int(num)):
                aid += 1
                entities["amounts"].append({
                    "id": f"a{aid}", "value": num, "currency": "",
                    "label": normalize_field_name(orig_k) or "value",
                    "confidence": 0.82,
                })
                continue

        # Text values — store as extra fields (no forced entity classification)
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


def parse_csv(file_bytes: bytes) -> dict:
    """Build cleaned plain text + metadata from CSV for the AI pipeline."""
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

    ALL rows processed through the universal generic path — no fixed schema
    processors (transaction, employee, sales). Optional AI mapping for enrichment.
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

    logger.info("CSV universal processing | headers=%s | rows=%s", fieldnames, len(rows))

    ai_mapping: dict[str, Any] | None = None
    ai_schema_type = "generic"

    # Try AI schema detection for enrichment (optional)
    if api_key and str(api_key).strip() and rows:
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
                        "CSV AI enrichment | schema=%s | roles=%s",
                        ai_schema_type, list(ai_mapping.keys()),
                    )
                else:
                    logger.warning("AI schema detection returned empty mapping; using universal path.")
            except Exception as e:
                logger.warning("AI schema detection skipped: %s", e)

    results: list[dict] = []
    MAX_ROWS = 1000

    for idx, raw_row in enumerate(rows):
        if idx >= MAX_ROWS:
            logger.warning("CSV exceeded maximum rows (%s). Truncating.", MAX_ROWS)
            break

        if not any(str(v).strip() for v in raw_row.values() if v is not None):
            continue

        row = clean_csv_row(dict(raw_row))

        try:
            if ai_mapping:
                # Use AI mapping to enrich the generic row
                doc = _process_ai_enriched_row(row, ai_mapping, ai_schema_type, idx + 1)
            else:
                doc = process_generic_tabular_row(row, idx + 1)
            results.append(doc)

        except Exception as e:
            logger.error("Error processing row %s: %s", idx, e)
            results.append({
                "document_id": f"error-{idx}",
                "document_type": "error",
                "status": "failed",
                "error": str(e),
                "entities": {"person_names": [], "organizations": [], "dates": [], "amounts": []},
                "relationships": [],
                "metadata": {"file_type": "csv"},
            })

    return results


def _process_ai_enriched_row(
    row: dict,
    mapping: dict[str, Any],
    schema_type: str,
    row_index: int,
) -> dict:
    """Build one structured document using AI-derived column mapping (generic)."""
    m = dynamic_map_row(row, mapping)

    # Find a document ID from any identifier-like column
    doc_id = f"row-{row_index}"
    for k, v in row.items():
        nk = normalize_field_name(str(k))
        if v is not None and str(v).strip():
            # Use first non-empty value with high uniqueness as ID
            s = str(v).strip()[:120]
            if s:
                doc_id = s
                break

    entities: dict = {"person_names": [], "organizations": [], "dates": [], "amounts": []}

    # Map detected values to entities
    for role, value in m.items():
        if value in (None, ""):
            continue
        sval = str(value).strip()

        if role in ("date",) and sval:
            entities["dates"].append({"id": f"d1", "value": sval, "confidence": 0.82})
        elif role in ("amount_monetary", "amount", "salary_comp") and sval:
            num = safe_float(sval)
            if num is not None:
                entities["amounts"].append({
                    "id": f"a1", "value": num, "currency": "",
                    "label": "amount", "confidence": 0.86,
                })

    status = "success" if any(entities[k] for k in entities) else "partial"

    return {
        "document_id": doc_id,
        "document_type": "generic_csv",
        "status": status,
        "error": None,
        "entities": entities,
        "relationships": [],
        "metadata": {"file_type": "csv", "schema_source": "ai", "ai_schema_type": schema_type},
    }