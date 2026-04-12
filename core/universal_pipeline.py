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


def _mapping_covers_few_roles(mapping: dict[str, Any]) -> bool:
    """True when we should still try AI (heuristic too thin)."""
    from core.schema_inference import mapping_is_non_empty

    if not mapping_is_non_empty(mapping):
        return True
    filled = sum(1 for v in mapping.values() if v)
    return filled < 2


def _csv_cache_is_usable(cached: dict[str, Any]) -> bool:
    """Trust cache when it has a modern envelope or an explicit mapping dict (incl. empty)."""
    if "source" in cached:
        return True
    return isinstance(cached.get("mapping"), dict)


def _process_structured_csv(
    file_bytes: bytes,
    filename: str,
    api_key: str | None,
) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    import csv
    import io

    from core.schema_inference import (
        heuristic_row_without_mapping,
        infer_mapping_from_columns,
        mapping_is_non_empty,
        mapping_to_universal_row,
    )
    from parsers.csv_parser import clean_csv_row

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
    sample: list[dict[str, Any]] = []
    for raw in all_rows[:40]:
        if any(str(v).strip() for v in raw.values() if v is not None):
            sample.append(clean_csv_row(dict(raw)))
        if len(sample) >= 8:
            break

    memory_hit = False
    mapping: dict[str, Any] = {}
    schema_type = "generic"
    schema_source = "heuristic"

    if cached and _csv_cache_is_usable(cached):
        memory_hit = True
        mapping = dict(cached.get("mapping") or {})
        schema_type = str(cached.get("schema_type") or cached.get("handler") or "generic")
        schema_source = str(cached.get("source") or "memory")
        logger.info("Schema memory hit | cols=%s | mapped_roles=%s", len(columns), bool(mapping))

    if not memory_hit:
        mapping = infer_mapping_from_columns(columns)
        schema_type = str(
            (cached or {}).get("schema_type") or (cached or {}).get("handler") or "generic"
        )
        schema_source = "heuristic"

        if _mapping_covers_few_roles(mapping) and sample and api_key and str(api_key).strip():
            try:
                from ai_layer.schema_detector import detect_schema_ai

                ai = detect_schema_ai(sample, api_key)
                ai_m = ai.get("mapping") or {}
                if mapping_is_non_empty(ai_m):
                    mapping = ai_m
                    schema_type = str(ai.get("schema_type") or schema_type)
                    schema_source = "ai"
                    logger.info("Schema from AI | type=%s", schema_type)
            except Exception as ex:
                logger.warning("AI schema detection failed: %s", ex)

        payload = {
            "mapping": mapping,
            "schema_type": schema_type,
            "source": schema_source,
        }
        save_schema_to_memory(columns, payload)

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
            if mapping_is_non_empty(mapping):
                data_rows.append(
                    mapping_to_universal_row(row, mapping, schema_source=schema_source)
                )
            else:
                data_rows.append(heuristic_row_without_mapping(row))
        except Exception as ex:
            logger.exception("CSV row error: %s", ex)
            status = "partial"
            data_rows.append(build_clean_row(None, None, None, None, 0.2))

    if not data_rows and status == "success":
        status = "partial"

    meta = {
        "file_type": "csv",
        "document_type": schema_type,
        "column_count": len(columns),
        "schema_source": schema_source,
        "from_cache": memory_hit,
        "raw_text": content[:100_000],
    }
    return data_rows, meta, status


def _process_unstructured(
    file_bytes: bytes,
    filename: str,
    api_key: str | None,
) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    from parsers.router import route_file
    from parsers.txt_parser import ParseError

    from ai_layer.extractor import AIServiceError, extract_entities
    from core.fallback_extractor import fallback_extract
    from orchestrator import MAX_AI_CHARS, detect_document_type, sample_csv_text
    from post_processor.processor import ValidationError, post_process

    ext = Path(filename).suffix.lower().lstrip(".") or "txt"
    try:
        parsed = route_file(file_bytes, ext)
    except ParseError as e:
        rows = fallback_extract("")
        return (
            rows,
            {
                "file_type": ext,
                "document_type": "unknown",
                "error": str(e),
                "raw_text": "",
                "extraction": "fallback",
            },
            "partial",
        )

    document_type = detect_document_type(parsed["text"], filename)
    if ext == "csv":
        text_for_ai = sample_csv_text(parsed["text"], MAX_AI_CHARS)
    else:
        text_for_ai = parsed["text"][:MAX_AI_CHARS]

    raw_slice = parsed.get("text", "")[:500_000]
    base_meta: dict[str, Any] = {
        "file_type": ext,
        "document_type": document_type,
        "word_count": parsed.get("metadata", {}).get("word_count"),
        "raw_text": raw_slice,
    }

    def _with_fallback(err: str | None) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
        logger.warning("AI failed, using fallback extraction")
        rows_fb = fallback_extract(text_for_ai)
        meta_fb = {**base_meta, "extraction": "fallback"}
        if err:
            meta_fb["error"] = err
        logger.info("Fallback extraction completed")
        return rows_fb, meta_fb, "partial"

    if not api_key or not str(api_key).strip():
        rows_fb = fallback_extract(text_for_ai)
        meta_fb = {
            **base_meta,
            "extraction": "fallback",
            "error": "GEMINI_API_KEY not set; heuristic extraction only.",
        }
        logger.info("Fallback extraction completed (no API key)")
        return rows_fb, meta_fb, "partial"

    try:
        ai_output = extract_entities(
            text_for_ai,
            api_key=api_key,
            document_type=document_type,
        )
    except AIServiceError as e:
        return _with_fallback(str(e))

    try:
        result = post_process(
            ai_output,
            source_file=filename,
            file_metadata=parsed["metadata"],
        )
    except ValidationError as e:
        return _with_fallback(str(e))
    except Exception as e:
        logger.exception("Unstructured post_process error")
        return _with_fallback(str(e))

    rows = entities_to_universal_rows(result.get("entities") or {})
    meta = {
        **base_meta,
        "document_type": result.get("document_type", document_type),
        "extraction": "ai",
    }
    if result.get("error"):
        meta["error"] = str(result["error"])
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
        data, meta, st = _process_unstructured(file_bytes, filename, api_key)
    else:
        data, meta, st = _process_structured_csv(file_bytes, filename, api_key)

    mark_amount_outliers(data)

    if not data:
        from core.fallback_extractor import fallback_extract

        logger.warning("Empty data after processing; injecting fallback placeholder")
        data = fallback_extract("")
        st = "partial"

    final_status = st
    if final_status == "failed" and data:
        final_status = "partial"

    envelope: dict[str, Any] = {
        "document_id": document_id,
        "document_type": str(meta.get("document_type", "auto")),
        "status": final_status,
        "data": data,
        "error": meta.get("error"),
        "metadata": {
            "file_type": meta.get("file_type", "unknown"),
            "row_count": len(data),
            "processed_at": processed_at,
        },
    }
    if meta.get("extraction"):
        envelope["metadata"]["extraction"] = meta["extraction"]
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
