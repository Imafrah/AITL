"""
Final cleaning layer: schema-complete rows, invalid-value repair, numeric imputation,
row-quality filtering, normalization, and duplicate-row removal — fully dataset-agnostic.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import re
from pathlib import Path
from typing import Any

from core.cleaning import (
    amount_from_value,
    clean_name,
    is_valid_date,
    is_valid_email,
    normalize_city,
    normalize_date_value,
    normalize_status_value,
)
from core.schema_cleanup import infer_critical_fields, validate_row_numeric_aggregate
from parsers.csv_parser import normalize_field_name

logger = logging.getLogger(__name__)

_RESERVED_KEYS = frozenset(
    {
        "confidence",
        "is_anomaly",
        "is_valid_email",
        "is_valid_date",
        "is_valid_numeric",
        "is_outlier",
    }
)

# Keys used only for QC / scoring — excluded from "missing rate" so rows are not
# penalized for synthetic boolean fields.
_RATIO_EXCLUDED_KEYS = _RESERVED_KEYS | frozenset({"is_outlier"})


def enforce_schema(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Collect every key seen across the dataset and ensure each record contains all keys.
    Missing entries are set to None.
    """
    all_keys: list[str] = []
    seen: set[str] = set()
    for r in records:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                all_keys.append(k)
    out: list[dict[str, Any]] = []
    for r in records:
        row = dict(r)
        for k in all_keys:
            if k not in row:
                row[k] = None
        out.append(row)
    return out


def _is_present(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, str) and not v.strip():
        return False
    return True


def _is_email_key(key: str) -> bool:
    nk = normalize_field_name(key)
    return "email" in nk or nk in ("e_mail", "mail")


def _is_name_like_key(key: str) -> bool:
    nk = normalize_field_name(key)
    if not nk:
        return False
    if "company" in nk or "org" in nk or "organization" in nk:
        return False
    return "name" in nk or nk in ("person", "customer", "employee", "full_name")


def _is_city_like_key(key: str) -> bool:
    nk = normalize_field_name(key)
    return any(x in nk for x in ("city", "town", "municipality", "location", "address"))


def _is_status_like_key(key: str) -> bool:
    nk = normalize_field_name(key)
    return "status" in nk or nk in ("state", "stage")


def _coerce_number(value: Any) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if value != value:  # NaN
            return None
        if isinstance(value, float) and value == int(value) and abs(value) < 1e12:
            return int(value)
        return float(value)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # Optional: spelled-out small integers (very basic, no hardcoded dataset types)
        low = s.lower().replace("-", " ")
        word_map = {
            "zero": 0,
            "one": 1,
            "two": 2,
            "three": 3,
            "four": 4,
            "five": 5,
            "six": 6,
            "seven": 7,
            "eight": 8,
            "nine": 9,
            "ten": 10,
        }
        parts = [p for p in re.split(r"\s+", low) if p]
        if parts and all(p in word_map for p in parts) and len(parts) <= 3:
            total = 0
            for p in parts:
                total += word_map[p]
            return total
        n = amount_from_value(s)
        if n is not None:
            if n == int(n) and abs(n) < 1e12:
                return int(n)
            return float(n)
    return None


def _detect_numeric_columns(records: list[dict[str, Any]]) -> set[str]:
    """Mark columns that are mostly numeric (values or parseable strings)."""
    if not records:
        return set()
    keys = [k for k in records[0].keys() if k not in _RESERVED_KEYS and not str(k).startswith("_")]
    numeric_cols: set[str] = set()
    n = len(records)
    for k in keys:
        filled = 0
        num_ok = 0
        for r in records:
            v = r.get(k)
            if not _is_present(v):
                continue
            filled += 1
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                num_ok += 1
            elif isinstance(v, str):
                if _coerce_number(v) is not None:
                    num_ok += 1
        if filled < max(2, min(3, n // 2 or 1)):
            continue
        if num_ok / filled >= 0.55:
            numeric_cols.add(k)
    return numeric_cols


def _median(vals: list[float]) -> float | None:
    if not vals:
        return None
    s = sorted(vals)
    m = len(s) // 2
    if len(s) % 2:
        return float(s[m])
    return float(s[m - 1] + s[m]) / 2.0


def _mean(vals: list[float]) -> float | None:
    if not vals:
        return None
    return sum(vals) / len(vals)


def _normalize_strings_inplace(record: dict[str, Any]) -> None:
    for k, v in list(record.items()):
        if k in _RESERVED_KEYS:
            continue
        if isinstance(v, str):
            s = v.strip()
            if not s:
                record[k] = None
                continue
            nk = normalize_field_name(k)
            if _is_city_like_key(k):
                record[k] = normalize_city(s)
            elif _is_name_like_key(k):
                record[k] = clean_name(s)
            elif _is_status_like_key(k):
                record[k] = normalize_status_value(s)
            elif "date" in nk or nk in ("dob", "birth", "timestamp", "created", "updated"):
                iso = normalize_date_value(s)
                record[k] = iso if iso else (s[:128] if s else None)
            else:
                record[k] = s


def _refresh_validation_flags(record: dict[str, Any]) -> None:
    em = record.get("email")
    record["is_valid_email"] = bool(em and is_valid_email(em))
    dt = record.get("date")
    if dt is not None and str(dt).strip():
        record["is_valid_date"] = is_valid_date(dt)
    else:
        record["is_valid_date"] = True
    record["is_valid_numeric"] = validate_row_numeric_aggregate(record)


def _missing_ratio(record: dict[str, Any], schema_keys: list[str]) -> float:
    denom_keys = [k for k in schema_keys if k not in _RATIO_EXCLUDED_KEYS]
    if not denom_keys:
        return 0.0
    missing = sum(1 for k in denom_keys if not _is_present(record.get(k)))
    return missing / len(denom_keys)


def _dedupe_identical_rows(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for r in records:
        fp = json.dumps(r, sort_keys=True, default=str, ensure_ascii=False)
        if fp in seen:
            continue
        seen.add(fp)
        out.append(r)
    return out, len(records) - len(out)


def run_final_cleaning_layer(
    records: list[dict[str, Any]],
    *,
    critical_fields: list[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Full production pass on a **copy** of rows (caller may keep the original for
    intermediate/anomaly views). Returns ``(cleaned_rows, stats)``.
    """
    stats: dict[str, Any] = {
        "rows_in": len(records),
        "rows_out": 0,
        "invalid_values_replaced": False,
        "numeric_imputed_median": False,
        "numeric_imputed_mean": False,
        "low_quality_removed": 0,
        "duplicate_rows_removed": 0,
        "critical_fields": [],
    }
    if not records:
        return [], stats

    working = copy.deepcopy(records)
    working = enforce_schema(working)
    logger.info("Schema enforced across all records")

    schema_keys = list(working[0].keys())
    numeric_cols = _detect_numeric_columns(working)
    critical = list(critical_fields) if critical_fields else infer_critical_fields(working)
    stats["critical_fields"] = critical

    invalid_changed = False
    for r in working:
        for k in list(r.keys()):
            if k in _RESERVED_KEYS:
                continue
            v = r.get(k)
            if _is_email_key(k) and v is not None and str(v).strip():
                if not is_valid_email(str(v)):
                    r[k] = None
                    invalid_changed = True
            elif k in numeric_cols:
                if v is None or (isinstance(v, str) and not str(v).strip()):
                    r[k] = None
                else:
                    coerced = _coerce_number(v)
                    if coerced is None and _is_present(v):
                        invalid_changed = True
                    r[k] = coerced
    if invalid_changed:
        logger.info("Invalid values replaced")

    filled_median = False
    filled_mean = False
    for k in numeric_cols:
        vals: list[float] = []
        for r in working:
            v = r.get(k)
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                vals.append(float(v))
        med = _median(vals)
        mean_v = _mean(vals)
        fill_value: float | int | None = med if med is not None else mean_v
        if fill_value is None:
            continue
        if med is not None:
            filled_median = True
        elif mean_v is not None:
            filled_mean = True
        for r in working:
            if r.get(k) is None:
                if isinstance(fill_value, float) and fill_value == int(fill_value):
                    r[k] = int(fill_value)
                else:
                    r[k] = fill_value

    if filled_median:
        logger.info("Missing values filled using median")
    if filled_mean and not filled_median:
        logger.info("Missing values filled using mean")

    stats["numeric_imputed_median"] = filled_median
    stats["numeric_imputed_mean"] = filled_mean and not filled_median

    before_filter = len(working)
    kept: list[dict[str, Any]] = []
    for r in working:
        if _missing_ratio(r, schema_keys) > 0.5:
            continue
        drop = False
        for cf in critical:
            if not _is_present(r.get(cf)):
                drop = True
                break
        if not drop:
            kept.append(r)
    removed = before_filter - len(kept)
    if removed:
        logger.info("Low-quality rows removed")
    stats["low_quality_removed"] = removed
    working = kept

    for r in working:
        _normalize_strings_inplace(r)
        _refresh_validation_flags(r)
        # Surviving rows passed repair + QC filters; stale anomaly flags are misleading downstream.
        r["is_anomaly"] = False

    working, dup_removed = _dedupe_identical_rows(working)
    stats["duplicate_rows_removed"] = dup_removed

    working = enforce_schema(working)
    stats["invalid_values_replaced"] = invalid_changed
    stats["rows_out"] = len(working)
    return working, stats


def write_cleaning_outputs(
    document_id: str,
    intermediate_payload: dict[str, Any],
    final_rows: list[dict[str, Any]],
    *,
    output_dir: str | os.PathLike[str] | None = None,
) -> dict[str, str]:
    """
    Write ``intermediate_output.json`` (validation + anomaly context) and
    ``final_cleaned_output.json`` (production rows). Returns paths written.
    """
    root = Path(output_dir or os.getenv("AITL_OUTPUT_DIR", "output"))
    safe_id = str(document_id).replace("..", "").replace("/", "_").replace("\\", "_")[:200]
    base = root / safe_id
    base.mkdir(parents=True, exist_ok=True)
    inter_path = base / "intermediate_output.json"
    final_path = base / "final_cleaned_output.json"

    inter_body = {
        "document_id": document_id,
        "data": intermediate_payload.get("data"),
        "metadata": intermediate_payload.get("metadata"),
    }
    inter_path.write_text(
        json.dumps(inter_body, indent=2, default=str, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    final_body = {
        "document_id": document_id,
        "row_count": len(final_rows),
        "data": final_rows,
    }
    final_path.write_text(
        json.dumps(final_body, indent=2, default=str, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    logger.info("Wrote intermediate output | path=%s", inter_path)
    logger.info("Wrote final cleaned output | path=%s", final_path)
    return {"intermediate": str(inter_path.resolve()), "final": str(final_path.resolve())}
