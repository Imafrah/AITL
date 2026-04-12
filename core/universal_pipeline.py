"""
Universal file intelligence pipeline — hybrid rules + AI, SQLite schema memory,
row-level universal output.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from core.cleaning import build_clean_row, mark_amount_outliers
from core.file_router import route_file as classify_file
from core.output_formatter import to_table
from core.schema_memory import get_schema_from_memory, save_schema_to_memory

logger = logging.getLogger(__name__)

MAX_CSV_ROWS = int(__import__("os").getenv("AITL_MAX_CSV_ROWS", "100000"))


def structured_doc_to_row(doc: dict[str, Any]) -> dict[str, Any]:
    """One legacy structured document → one universal row."""
    e = doc.get("entities") or {}
    confs: list[float] = []

    p0 = (e.get("person_names") or [None])[0]
    if p0:
        pn = p0.get("value")
        confs.append(float(p0.get("confidence", 0.9)))
    else:
        pn = None

    o0 = (e.get("organizations") or [None])[0]
    if o0:
        org = o0.get("value")
        confs.append(float(o0.get("confidence", 0.88)))
    else:
        org = None

    d0 = (e.get("dates") or [None])[0]
    if d0:
        dv = d0.get("value")
        confs.append(float(d0.get("confidence", 0.82)))
    else:
        dv = None

    a0 = (e.get("amounts") or [None])[0]
    amt = None
    if a0:
        amt = a0.get("value")
        confs.append(float(a0.get("confidence", 0.86)))

    conf = sum(confs) / len(confs) if confs else 0.75
    return build_clean_row(pn, org, amt, dv, conf)


def generic_doc_to_rows(doc: dict[str, Any]) -> list[dict[str, Any]]:
    """Expand generic_csv-style multi-entity document into universal rows."""
    e = doc.get("entities") or {}
    p = e.get("person_names") or []
    o = e.get("organizations") or []
    d = e.get("dates") or []
    a = e.get("amounts") or []
    n = max(len(p), len(o), len(d), len(a), 1)
    rows: list[dict[str, Any]] = []
    for i in range(n):
        pi = p[i] if i < len(p) else None
        oi = o[i] if i < len(o) else None
        di = d[i] if i < len(d) else None
        ai = a[i] if i < len(a) else None
        confs = [x for x in [pi, oi, di, ai] if x]
        conf = (
            sum(float(x.get("confidence", 0.8)) for x in confs) / len(confs) if confs else 0.7
        )
        rows.append(
            build_clean_row(
                pi.get("value") if pi else None,
                oi.get("value") if oi else None,
                ai.get("value") if ai else None,
                di.get("value") if di else None,
                conf,
            )
        )
    if not any(r.get("person_name") or r.get("organization") or r.get("amount") for r in rows):
        return [build_clean_row(None, None, None, None, 0.5)]
    return rows


def document_to_universal_rows(doc: dict[str, Any]) -> list[dict[str, Any]]:
    dt = (doc.get("document_type") or "").lower()
    if dt == "generic_csv":
        return generic_doc_to_rows(doc)
    return [structured_doc_to_row(doc)]


def entities_to_universal_rows(entities: dict[str, Any]) -> list[dict[str, Any]]:
    """Post-process / AI-style entity lists → universal rows (unstructured)."""
    p = entities.get("person_names") or []
    o = entities.get("organizations") or []
    d = entities.get("dates") or []
    a = entities.get("amounts") or []
    n = max(len(p), len(o), len(d), len(a), 1)
    out: list[dict[str, Any]] = []
    for i in range(n):
        pi = p[i] if i < len(p) else None
        oi = o[i] if i < len(o) else None
        di = d[i] if i < len(d) else None
        ai = a[i] if i < len(a) else None
        parts = [x for x in [pi, oi, di, ai] if x]
        conf = (
            sum(float(x.get("confidence", 0.85)) for x in parts) / len(parts) if parts else 0.75
        )
        out.append(
            build_clean_row(
                pi.get("value") if pi else None,
                oi.get("value") if oi else None,
                ai.get("value") if ai else None,
                di.get("value") if di else None,
                conf,
            )
        )
    if not any(
        r.get("person_name") or r.get("organization") or r.get("amount") for r in out
    ):
        out = [build_clean_row(None, None, None, None, 0.5)]
    return out


def _process_structured_csv(
    file_bytes: bytes,
    filename: str,
    api_key: str | None,
) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    import csv
    import io

    from parsers.csv_parser import (
        _apply_aliases,
        _detect_csv_schema,
        _EMPLOYEE_ALIASES,
        _SALES_ALIASES,
        _TRANSACTION_ALIASES,
        clean_csv_row,
        process_ai_mapped_row,
        process_employee_row,
        process_generic_tabular_row,
        process_sales_row,
        process_transaction_row,
    )

    try:
        content = file_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        content = file_bytes.decode("latin-1")

    reader = csv.DictReader(io.StringIO(content))
    raw_headers = reader.fieldnames or []
    columns = [str(f).strip() for f in raw_headers if f is not None and str(f).strip()]
    all_rows = list(reader)

    if not columns:
        return [], {"file_type": "csv", "document_type": "unknown", "raw_text": content[:100_000]}, "failed"

    cached = get_schema_from_memory(columns)
    rule_schema = _detect_csv_schema(columns)
    effective: dict[str, Any] | None = None

    if cached:
        logger.info("Using cached schema (memory hit) | cols=%s", len(columns))
        effective = cached
    elif rule_schema != "generic":
        effective = {"handler": rule_schema, "mapping": None, "schema_type": rule_schema}
        save_schema_to_memory(columns, effective)
        logger.info("Schema saved (rule-based) | handler=%s", rule_schema)
    else:
        sample: list[dict[str, Any]] = []
        for raw in all_rows[:40]:
            if any(str(v).strip() for v in raw.values() if v is not None):
                sample.append(clean_csv_row(dict(raw)))
            if len(sample) >= 5:
                break
        effective = {"handler": "generic", "mapping": None, "schema_type": "generic"}
        if sample and api_key and str(api_key).strip():
            try:
                from ai_layer.schema_detector import detect_schema_ai

                ai = detect_schema_ai(sample, api_key)
                m = ai.get("mapping") or {}
                if m:
                    effective = {
                        "handler": "mapped",
                        "mapping": m,
                        "schema_type": ai.get("schema_type", "generic"),
                    }
                    save_schema_to_memory(columns, effective)
                    logger.info("Schema saved (AI) | type=%s", effective["schema_type"])
            except Exception as ex:
                logger.warning("AI schema detection failed: %s", ex)

    assert effective is not None
    handler = effective.get("handler") or "generic"
    mapping = effective.get("mapping") or {}
    schema_type = effective.get("schema_type") or "generic"

    data_rows: list[dict[str, Any]] = []
    status = "success"

    for idx, raw_row in enumerate(all_rows):
        if idx >= MAX_CSV_ROWS:
            logger.warning("CSV truncated at %s rows", MAX_CSV_ROWS)
            break
        if not any(str(v).strip() for v in raw_row.values() if v is not None):
            continue
        row = clean_csv_row(dict(raw_row))
        try:
            if handler == "transaction":
                doc = process_transaction_row(_apply_aliases(row, _TRANSACTION_ALIASES), idx + 1)
            elif handler == "employee":
                doc = process_employee_row(_apply_aliases(row, _EMPLOYEE_ALIASES), idx + 1)
            elif handler == "sales":
                doc = process_sales_row(_apply_aliases(row, _SALES_ALIASES), idx + 1)
            elif handler == "mapped" and mapping:
                doc = process_ai_mapped_row(row, mapping, str(schema_type), idx + 1)
            else:
                doc = process_generic_tabular_row(row, idx + 1)
            if doc.get("status") == "failed":
                status = "partial"
            data_rows.extend(document_to_universal_rows(doc))
        except Exception as ex:
            logger.exception("CSV row error: %s", ex)
            status = "partial"
            data_rows.append(build_clean_row(None, None, None, None, 0.2))

    if not data_rows and status == "success":
        status = "partial"

    doc_type = str(schema_type) if handler == "mapped" else handler
    meta = {
        "file_type": "csv",
        "document_type": doc_type,
        "column_count": len(columns),
        "schema_handler": handler,
        "from_cache": bool(cached),
        "raw_text": content[:100_000],
    }
    return data_rows, meta, status


def _process_unstructured(
    file_bytes: bytes,
    filename: str,
    api_key: str,
) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    from parsers.router import route_file
    from parsers.txt_parser import ParseError

    from ai_layer.extractor import AIServiceError, extract_entities
    from orchestrator import MAX_AI_CHARS, detect_document_type, sample_csv_text
    from post_processor.processor import ValidationError, post_process

    ext = Path(filename).suffix.lower().lstrip(".") or "txt"
    try:
        parsed = route_file(file_bytes, ext)
    except ParseError as e:
        return [], {"file_type": ext, "document_type": "unknown", "error": str(e), "raw_text": ""}, "failed"

    document_type = detect_document_type(parsed["text"], filename)
    if ext == "csv":
        text_for_ai = sample_csv_text(parsed["text"], MAX_AI_CHARS)
    else:
        text_for_ai = parsed["text"][:MAX_AI_CHARS]

    try:
        ai_output = extract_entities(
            text_for_ai,
            api_key=api_key,
            document_type=document_type,
        )
        result = post_process(
            ai_output,
            source_file=filename,
            file_metadata=parsed["metadata"],
        )
    except (AIServiceError, ValidationError) as e:
        return (
            [],
            {
                "file_type": ext,
                "document_type": document_type,
                "error": str(e),
                "raw_text": parsed.get("text", "")[:500_000],
            },
            "partial",
        )
    except Exception as e:
        logger.exception("Unstructured pipeline error")
        return (
            [],
            {
                "file_type": ext,
                "document_type": document_type,
                "error": str(e),
                "raw_text": parsed.get("text", "")[:500_000],
            },
            "failed",
        )

    rows = entities_to_universal_rows(result.get("entities") or {})
    meta = {
        "file_type": ext,
        "document_type": result.get("document_type", document_type),
        "word_count": parsed.get("metadata", {}).get("word_count"),
        "raw_text": parsed.get("text", "")[:500_000],
    }
    return rows, meta, str(result.get("status", "success"))


def process_universal(
    file_bytes: bytes,
    filename: str,
    output_format: str,
    api_key: str | None,
) -> dict[str, Any]:
    """
    Main entry: returns universal envelope. ``output_format`` in json | table | csv
    (csv only affects optional table blob; route may return raw bytes for csv).
    """
    kind = classify_file(filename or "")
    document_id = str(uuid.uuid4())
    processed_at = datetime.now(timezone.utc).isoformat()

    if kind == "unknown":
        return {
            "document_id": document_id,
            "document_type": "unknown",
            "status": "failed",
            "error": "Unsupported file type.",
            "data": [],
            "metadata": {
                "file_type": "unknown",
                "row_count": 0,
                "processed_at": processed_at,
            },
        }

    if kind == "unstructured":
        if not api_key or not str(api_key).strip():
            return {
                "document_id": document_id,
                "document_type": "unknown",
                "status": "failed",
                "error": "GEMINI_API_KEY is required for TXT/PDF processing.",
                "data": [],
                "metadata": {
                    "file_type": Path(filename or "").suffix.lower().lstrip(".") or "txt",
                    "row_count": 0,
                    "processed_at": processed_at,
                },
            }
        data, meta, st = _process_unstructured(file_bytes, filename, api_key)
    else:
        data, meta, st = _process_structured_csv(file_bytes, filename, api_key)

    mark_amount_outliers(data)

    if not data:
        st = "failed" if st == "failed" else "partial"

    envelope: dict[str, Any] = {
        "document_id": document_id,
        "document_type": str(meta.get("document_type", "auto")),
        "status": st,
        "data": data,
        "metadata": {
            "file_type": meta.get("file_type", "unknown"),
            "row_count": len(data),
            "processed_at": processed_at,
        },
    }
    if meta.get("error"):
        envelope["error"] = meta["error"]
    if output_format == "table":
        envelope["table"] = to_table(data)

    # Persist universal envelope (best-effort)
    try:
        from db.crud import save_document

        save_document(
            document_id=document_id,
            source_file=filename or "upload",
            document_type=str(envelope["document_type"]),
            status=envelope["status"],
            raw_text=str(meta.get("raw_text") or "")[:500_000],
            structured_output=envelope,
        )
    except Exception as e:
        logger.warning("Could not persist document: %s", e)

    return envelope
