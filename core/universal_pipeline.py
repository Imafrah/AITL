"""
Universal file intelligence pipeline — hybrid rules + AI, SQLite schema memory,
row-level universal output.

Flow: parse → clean → validate → analyze → **final cleaning** (repair + schema-unify).

All decisions driven by DatasetProfile — no hardcoded column names or schema types.
"""

from __future__ import annotations

import copy
import logging
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from core.analytics_engine import compute_analytics
from core.anomaly_detector import apply_anomaly_detection
from core.cleaning import (
    is_valid_date,
    is_valid_email,
    is_valid_phone,
    is_valid_salary,
)
from core.dashboard_formatter import build_dashboard
from core.data_profiler import (
    detect_field_types,
    profile_dataset,
    _is_present,
)
from core.final_cleaning import run_final_cleaning_layer, write_cleaning_outputs
from core.file_router import route_file as classify_file
from core.intelligence_record import coerce_intelligence_row, dedupe_intelligence_rows
from core.output_formatter import to_table
from core.schema_cleanup import (
    clean_schema,
    compute_adaptive_confidence,
    infer_critical_fields,
    validate_row_numeric_aggregate,
)
from core.schema_memory import get_schema_from_memory, save_schema_to_memory

logger = logging.getLogger(__name__)

MAX_CSV_ROWS = int(__import__("os").getenv("AITL_MAX_CSV_ROWS", "100000"))


def _is_numeric_like(v: Any) -> bool:
    if isinstance(v, bool):
        return False
    if isinstance(v, (int, float)):
        return v == v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return False
        if re.fullmatch(r"[\s$€£₹-]*\d[\d,.\s$€£₹%-]*", s):
            return True
    return False


def _build_validation_summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    """Build validation summary using dynamic field type detection."""
    field_types = detect_field_types(rows) if rows else {}
    numeric_cols = set(field_types.get("numeric", set())) | set(field_types.get("monetary", set()))
    date_cols = set(field_types.get("date", set()))
    email_cols = set(field_types.get("email", set()))
    phone_cols = set(field_types.get("phone", set()))

    valid_numeric_cells = 0
    valid_salary_cells = 0
    for r in rows:
        for k in numeric_cols:
            val = r.get(k)
            if _is_numeric_like(val):
                valid_numeric_cells += 1
                if is_valid_salary(val):
                    valid_salary_cells += 1

    valid_date_rows = 0
    if date_cols:
        for r in rows:
            if all((not _is_present(r.get(k))) or is_valid_date(r.get(k)) for k in date_cols):
                valid_date_rows += 1
    else:
        valid_date_rows = len(rows)

    valid_email_count = 0
    if email_cols:
        for r in rows:
            if any(is_valid_email(r.get(k)) for k in email_cols):
                valid_email_count += 1

    valid_phone_count = 0
    if phone_cols:
        for r in rows:
            if any(is_valid_phone(r.get(k)) for k in phone_cols):
                valid_phone_count += 1

    return {
        "valid_email_count": valid_email_count,
        "valid_phone_count": valid_phone_count,
        "valid_salary_count": valid_salary_cells,
        "valid_date_count": valid_date_rows,
        "valid_numeric_count": valid_numeric_cells,
    }


def structured_doc_to_row(doc: dict[str, Any]) -> dict[str, Any]:
    """One legacy structured document → one universal row."""
    e = doc.get("entities") or {}
    row: dict[str, Any] = {}

    # Extract all entity types generically
    for entity_type, items in e.items():
        if not isinstance(items, list) or not items:
            continue
        first = items[0]
        if isinstance(first, dict) and "value" in first:
            row[entity_type] = first["value"]

    row["confidence"] = 0.75
    row["is_anomaly"] = False
    row["is_valid_email"] = True
    row["is_valid_date"] = True
    row["is_valid_numeric"] = True

    return row


def generic_doc_to_rows(doc: dict[str, Any]) -> list[dict[str, Any]]:
    """Expand generic multi-entity document into universal rows."""
    e = doc.get("entities") or {}
    
    # Find all entity lists and their lengths
    entity_lists: dict[str, list] = {}
    for key, items in e.items():
        if isinstance(items, list) and items:
            entity_lists[key] = items

    if not entity_lists:
        return [{"confidence": 0.5, "is_anomaly": False, "is_valid_email": True, "is_valid_date": True, "is_valid_numeric": True}]

    n = max(len(v) for v in entity_lists.values())
    rows: list[dict[str, Any]] = []

    for i in range(n):
        row: dict[str, Any] = {}
        confs: list[float] = []
        for key, items in entity_lists.items():
            item = items[i] if i < len(items) else None
            if item and isinstance(item, dict):
                row[key] = item.get("value")
                confs.append(float(item.get("confidence", 0.8)))

        row["confidence"] = sum(confs) / len(confs) if confs else 0.7
        row["is_anomaly"] = False
        row["is_valid_email"] = True
        row["is_valid_date"] = True
        row["is_valid_numeric"] = True
        rows.append(row)

    if not any(any(v for k, v in r.items() if k not in ("confidence", "is_anomaly", "is_valid_email", "is_valid_date", "is_valid_numeric")) for r in rows):
        return [{"confidence": 0.5, "is_anomaly": False, "is_valid_email": True, "is_valid_date": True, "is_valid_numeric": True}]

    return rows


def document_to_universal_rows(doc: dict[str, Any]) -> list[dict[str, Any]]:
    dt = (doc.get("document_type") or "").lower()
    if dt == "generic_csv":
        return generic_doc_to_rows(doc)
    return [structured_doc_to_row(doc)]


def entities_to_universal_rows(entities: dict[str, Any]) -> list[dict[str, Any]]:
    """Post-process / AI-style entity lists → universal rows."""
    entity_lists: dict[str, list] = {}
    for key, items in entities.items():
        if isinstance(items, list) and items:
            entity_lists[key] = items

    if not entity_lists:
        return [{"confidence": 0.75, "is_anomaly": False, "is_valid_email": True, "is_valid_date": True, "is_valid_numeric": True}]

    n = max(len(v) for v in entity_lists.values())
    out: list[dict[str, Any]] = []

    for i in range(n):
        row: dict[str, Any] = {}
        parts: list[float] = []
        for key, items in entity_lists.items():
            item = items[i] if i < len(items) else None
            if item and isinstance(item, dict):
                row[key] = item.get("value")
                parts.append(float(item.get("confidence", 0.85)))

        row["confidence"] = sum(parts) / len(parts) if parts else 0.75
        row["is_anomaly"] = False
        row["is_valid_email"] = True
        row["is_valid_date"] = True
        row["is_valid_numeric"] = True
        out.append(row)

    if not any(any(v for k, v in r.items() if k not in ("confidence", "is_anomaly", "is_valid_email", "is_valid_date", "is_valid_numeric")) for r in out):
        out = [{"confidence": 0.5, "is_anomaly": False, "is_valid_email": True, "is_valid_date": True, "is_valid_numeric": True}]

    return out


def _csv_cache_is_usable(cached: dict[str, Any]) -> bool:
    if "source" in cached:
        return True
    return isinstance(cached.get("mapping"), dict)


def _process_structured_csv(
    file_bytes: bytes,
    filename: str,
    api_key: str | None,
) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    from core.intelligence_record import heuristic_intelligence_row, semantic_intelligence_row
    from core.semantic_mapping import (
        classify_fields,
        field_map_needs_ai,
        field_map_nonempty,
        merge_field_maps,
    )
    from parsers.csv_parser import clean_csv_row
    from parsers.csv_robust import parse_csv_text_to_rows

    try:
        content = file_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        content = file_bytes.decode("latin-1")

    columns, all_rows = parse_csv_text_to_rows(content)

    cached = get_schema_from_memory(columns)
    sample: list[dict[str, Any]] = []
    for raw in all_rows[:40]:
        if any(str(v).strip() for v in raw.values() if v is not None):
            sample.append(clean_csv_row(dict(raw)))
        if len(sample) >= 8:
            break

    memory_hit = False
    # Use profiler-driven classification with sample data
    base_fm = classify_fields(columns, sample_rows=sample if sample else None)
    valid_columns = {str(c).strip() for c in columns if c is not None and str(c).strip()}
    field_map: dict[str, list[str]] = {k: list(v) for k, v in base_fm.items() if v}
    schema_source = "heuristic"

    if cached and _csv_cache_is_usable(cached):
        memory_hit = True
        field_map = merge_field_maps(
            base_fm,
            cached.get("field_map") or cached.get("mapping"),
            valid_columns=valid_columns,
        )
        field_map = {k: v for k, v in field_map.items() if v}
        schema_source = str(cached.get("source") or "memory")

    if not memory_hit:
        if field_map_needs_ai(field_map) and sample and api_key and str(api_key).strip():
            try:
                from ai_layer.schema_detector import detect_schema_ai
                ai = detect_schema_ai(sample, api_key)
                ai_m = ai.get("mapping") or {}
                merged = merge_field_maps(field_map, ai_m, valid_columns=valid_columns)
                if field_map_nonempty(merged):
                    field_map = {k: v for k, v in merged.items() if v}
                    schema_source = "ai"
                    logger.info("Schema from AI | roles=%s", list(field_map.keys())[:15])
            except Exception as ex:
                logger.warning("AI schema detection failed: %s", ex)

        payload = {
            "field_map": field_map,
            "mapping": field_map,
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
            if field_map_nonempty(field_map):
                data_rows.append(
                    semantic_intelligence_row(
                        row, field_map, schema_source=schema_source,
                    )
                )
            else:
                data_rows.append(heuristic_intelligence_row(row))
        except Exception as ex:
            logger.exception("CSV row error: %s", ex)
            status = "partial"
            data_rows.append(coerce_intelligence_row({"confidence": 0.2}))

    if not data_rows and status == "success":
        status = "partial"

    meta = {
        "file_type": "csv",
        "document_type": "tabular",
        "semantic_map": field_map,
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
            text_for_ai, api_key=api_key, document_type=document_type,
        )
    except AIServiceError as e:
        return _with_fallback(str(e))

    try:
        result = post_process(
            ai_output, source_file=filename, file_metadata=parsed["metadata"],
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
    Main entry: returns universal envelope.

    Stages: parse → clean → validate → analyze → final cleaning.
    All decisions driven by DatasetProfile — no hardcoded schemas.
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
            "validated_output": [],
            "cleaned_data": [],
            "final_cleaned_output": [],
            "metadata": {
                "file_type": "unknown",
                "row_count": 0,
                "processed_at": processed_at,
                "analytics": compute_analytics([]),
                "validation": {
                    "valid_email_count": 0,
                    "valid_phone_count": 0,
                    "valid_salary_count": 0,
                    "valid_date_count": 0,
                    "valid_numeric_count": 0,
                },
                "cleaning_summary": {
                    "rows_removed": 0,
                    "values_filled": 0,
                    "invalid_values_fixed": 0,
                },
            },
        }

    if kind == "unstructured":
        data, meta, st = _process_unstructured(file_bytes, filename, api_key)
    else:
        data, meta, st = _process_structured_csv(file_bytes, filename, api_key)

    data = [coerce_intelligence_row(r) for r in data]
    data = dedupe_intelligence_rows(data)

    semantic_map = meta.get("semantic_map") or {}
    cleaned: list[dict[str, Any]] = []
    norm_flags: list[bool] = []
    for r in data:
        cr, had_norm = clean_schema(r, semantic_map)
        cr["is_valid_numeric"] = validate_row_numeric_aggregate(cr)
        cleaned.append(cr)
        norm_flags.append(had_norm)
    data = cleaned

    if semantic_map:
        logger.info("Semantic grouping applied")
    if any(norm_flags):
        logger.info("Schema normalized")

    critical = infer_critical_fields(data)
    meta["critical_fields"] = critical

    # Profile for dynamic column type detection
    profile = profile_dataset(data) if data else None
    email_cols = set()
    date_cols = set()
    if profile:
        for name, cp in profile.columns.items():
            if cp.inferred_type == "email":
                email_cols.add(name)
            elif cp.inferred_type == "date":
                date_cols.add(name)

    apply_anomaly_detection(
        data, critical_fields=critical,
        email_columns=email_cols if email_cols else None,
        date_columns=date_cols if date_cols else None,
    )
    _anomaly_n = sum(1 for r in data if r.get("is_anomaly"))
    if _anomaly_n:
        logger.info("Anomaly detected | affected_records=%s", _anomaly_n)

    any_conf_adj = False
    for r, had_norm in zip(data, norm_flags):
        conf, adj = compute_adaptive_confidence(
            r, critical, had_schema_normalization=had_norm
        )
        r["confidence"] = conf
        if adj:
            any_conf_adj = True
    if any_conf_adj:
        logger.info("Confidence adjusted dynamically")

    # Pass only confirmed monetary columns to analytics so rank/index columns
    # are not included in aggregate statistics (Analytics Safety Rule #5).
    confirmed_monetary: set[str] = set()
    if profile:
        confirmed_monetary = {
            name for name, cp in profile.columns.items()
            if cp.inferred_type == "monetary" and cp.semantic_confidence_high
        }
    analytics = compute_analytics(
        data,
        confirmed_numeric_cols=confirmed_monetary if confirmed_monetary else None,
    )
    validation_summary = _build_validation_summary(data)

    if not data:
        from core.fallback_extractor import fallback_extract
        logger.warning("Empty data after processing; injecting fallback placeholder")
        raw_fb = [coerce_intelligence_row(r) for r in fallback_extract("")]
        st = "partial"
        fb_clean, fb_norms = [], []
        for r in raw_fb:
            cr, hn = clean_schema(r, semantic_map)
            cr["is_valid_numeric"] = validate_row_numeric_aggregate(cr)
            fb_clean.append(cr)
            fb_norms.append(hn)
        data = fb_clean
        critical_fb = infer_critical_fields(data)
        meta["critical_fields"] = critical_fb
        apply_anomaly_detection(data, critical_fields=critical_fb)
        for r, hn in zip(data, fb_norms):
            conf, _ = compute_adaptive_confidence(
                r, critical_fb, had_schema_normalization=hn
            )
            r["confidence"] = conf
        analytics = compute_analytics(data)
        validation_summary = _build_validation_summary(data)

    final_status = st
    if final_status == "failed" and data:
        final_status = "partial"

    cleaned_data, cleaning_stats = run_final_cleaning_layer(data)
    validation_summary = _build_validation_summary(cleaned_data)
    critical_for_metadata = cleaning_stats.get("critical_fields_detected") or infer_critical_fields(cleaned_data)

    intermediate_metadata: dict[str, Any] = {
        "file_type": meta.get("file_type", "unknown"),
        "row_count": len(data),
        "processed_at": processed_at,
        "analytics": analytics,
        "validation": validation_summary,
        "critical_fields": critical_for_metadata,
    }
    if meta.get("extraction"):
        intermediate_metadata["extraction"] = meta["extraction"]
    if meta.get("semantic_map") is not None:
        intermediate_metadata["semantic_map"] = meta.get("semantic_map")

    output_paths: dict[str, str] = {}
    try:
        output_paths = write_cleaning_outputs(
            document_id,
            {"validated_output": copy.deepcopy(data), "metadata": intermediate_metadata},
            cleaned_data,
            cleaning_stats=cleaning_stats,
        )
    except Exception as e:
        logger.warning("Could not write cleaning output files: %s", e)

    envelope: dict[str, Any] = {
        "document_id": document_id,
        "document_type": str(meta.get("document_type", "auto")),
        "status": final_status,
        "data": data,
        "validated_output": data,
        "cleaned_data": cleaned_data,
        "final_cleaned_output": cleaned_data,
        "error": meta.get("error"),
        "metadata": {
            **intermediate_metadata,
            "final_cleaning": cleaning_stats,
            "cleaning_summary": cleaning_stats.get("cleaning_summary", {}),
            "cleaned_row_count": len(cleaned_data),
            "output_paths": output_paths,
        },
    }
    if output_format == "table":
        envelope["table"] = to_table(data)
    elif output_format == "dashboard":
        envelope["dashboard"] = build_dashboard(data, analytics)

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
