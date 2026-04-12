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
from core.schema_cleanup import validate_row_numeric_aggregate
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


_UNITS: dict[str, int] = {
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
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
}

_TENS: dict[str, int] = {
    "twenty": 20,
    "thirty": 30,
    "forty": 40,
    "fifty": 50,
    "sixty": 60,
    "seventy": 70,
    "eighty": 80,
    "ninety": 90,
}


def _parse_english_number_words(low: str) -> int | None:
    """
    Parse common English number phrases (dataset-agnostic wording, not column names).
    Examples: "twenty five" → 25, "one hundred" → 100, "twenty-five" → 25.
    """
    s = re.sub(r"[\s,]+", " ", low.lower().strip().replace("-", " "))
    if not s:
        return None
    words = [w for w in s.split() if w]
    if not words:
        return None
    if any(c.isdigit() for c in s):
        return None

    def consume_units(idx: int) -> tuple[int | None, int]:
        if idx >= len(words):
            return None, idx
        w = words[idx]
        if w in _UNITS:
            return _UNITS[w], idx + 1
        return None, idx

    def consume_tens_units(idx: int) -> tuple[int | None, int]:
        if idx >= len(words):
            return None, idx
        w = words[idx]
        if w in _TENS:
            total = _TENS[w]
            idx += 1
            u, idx2 = consume_units(idx)
            if u is not None and u < 10:
                return total + u, idx2
            return total, idx
        return consume_units(idx)

    # "one hundred [and] twenty five" style
    if "hundred" in words:
        hi = words.index("hundred")
        if hi == 0:
            return None
        if words[hi - 1] not in _UNITS:
            return None
        hundreds = _UNITS[words[hi - 1]]
        if hundreds == 0:
            return None
        rest_start = hi + 1
        if rest_start < len(words) and words[rest_start] == "and":
            rest_start += 1
        if rest_start >= len(words):
            return hundreds * 100
        sub = " ".join(words[rest_start:])
        sub_val = _parse_english_number_words(sub)
        if sub_val is None:
            return None
        return hundreds * 100 + sub_val

    total, idx = consume_tens_units(0)
    if total is None:
        return None
    if idx < len(words):
        return None
    return total


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
        low = s.lower()
        wn = _parse_english_number_words(low)
        if wn is not None:
            return wn
        n = amount_from_value(s)
        if n is not None:
            if n == int(n) and abs(n) < 1e12:
                return int(n)
            return float(n)
    return None


def _looks_like_email_string(s: str) -> bool:
    t = s.strip()
    return "@" in t and "." in t.split("@")[-1]


def detect_field_types(records: list[dict[str, Any]]) -> dict[str, set[str]]:
    """
    Classify columns from **value patterns** (digits, ``@``, parseability), not dataset names.

    Returns ``{"numeric": set(...), "email": set(...), "text": set(...)}``.
    Each non-reserved key appears in exactly one bucket.
    """
    out: dict[str, set[str]] = {"numeric": set(), "email": set(), "text": set()}
    if not records:
        return out

    keys = [
        k
        for k in records[0].keys()
        if k not in _RESERVED_KEYS and not str(k).startswith("_")
    ]
    n = len(records)

    for k in keys:
        filled = 0
        str_vals: list[str] = []
        at_like = 0
        valid_email_n = 0
        num_ok = 0

        for r in records:
            v = r.get(k)
            if not _is_present(v):
                continue
            filled += 1
            if isinstance(v, str):
                sv = v.strip()
                str_vals.append(sv)
                if _looks_like_email_string(sv):
                    at_like += 1
                if is_valid_email(sv):
                    valid_email_n += 1
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                num_ok += 1
            elif isinstance(v, str):
                sv = v.strip()
                if "@" in sv:
                    continue
                if _coerce_number(sv) is not None:
                    num_ok += 1

        if filled == 0:
            out["text"].add(k)
            continue

        email_ratio = at_like / filled
        valid_email_ratio = valid_email_n / filled
        num_ratio = num_ok / filled

        # Email: many cells look like addresses (``@`` + TLD) or mostly valid emails
        if email_ratio >= 0.35 or valid_email_ratio >= 0.25:
            out["email"].add(k)
            continue

        # Single-row inputs: infer from the present cell(s) using the same signals.
        if n == 1:
            if num_ratio >= 1.0 or (filled == 1 and num_ok == 1):
                out["numeric"].add(k)
            else:
                out["text"].add(k)
            continue

        if filled < max(2, min(3, n // 2 or 1)):
            out["text"].add(k)
            continue

        if num_ratio >= 0.55:
            out["numeric"].add(k)
        else:
            out["text"].add(k)

    return out


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
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Full production pass on a **copy** of rows (caller may keep the original for
    validated / annotated views). Returns ``(cleaned_rows, stats)``.
    """
    stats: dict[str, Any] = {
        "rows_in": len(records),
        "rows_out": 0,
        "invalid_values_replaced": False,
        "missing_values_filled": False,
        "low_quality_removed": 0,
        "duplicate_rows_removed": 0,
        "field_types": {"numeric": [], "email": [], "text": []},
    }
    if not records:
        return [], stats

    logger.info("Final cleaning started")

    working = copy.deepcopy(records)
    working = enforce_schema(working)
    logger.info("Schema enforced")

    schema_keys = list(working[0].keys())
    field_types = detect_field_types(working)
    stats["field_types"] = {
        "numeric": sorted(field_types["numeric"]),
        "email": sorted(field_types["email"]),
        "text": sorted(field_types["text"]),
    }
    numeric_cols = field_types["numeric"]
    email_cols = field_types["email"]

    invalid_changed = False
    for r in working:
        for k in list(r.keys()):
            if k in _RESERVED_KEYS:
                continue
            v = r.get(k)
            if k in email_cols and v is not None and str(v).strip():
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

    filled_any = False
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
        filled_any = True
        for r in working:
            if r.get(k) is None:
                if isinstance(fill_value, float) and fill_value == int(fill_value):
                    r[k] = int(fill_value)
                else:
                    r[k] = fill_value

    if filled_any:
        logger.info("Missing values filled")
    stats["missing_values_filled"] = filled_any

    before_filter = len(working)
    kept: list[dict[str, Any]] = []
    for r in working:
        if _missing_ratio(r, schema_keys) > 0.5:
            continue
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
    validated_payload: dict[str, Any],
    final_rows: list[dict[str, Any]],
    *,
    output_dir: str | os.PathLike[str] | None = None,
) -> dict[str, str]:
    """
    Write ``validated_output.json`` (flags + anomalies) and
    ``final_cleaned_output.json`` (production rows). Returns paths written.
    """
    root = Path(output_dir or os.getenv("AITL_OUTPUT_DIR", "output"))
    safe_id = str(document_id).replace("..", "").replace("/", "_").replace("\\", "_")[:200]
    base = root / safe_id
    base.mkdir(parents=True, exist_ok=True)
    inter_path = base / "validated_output.json"
    final_path = base / "final_cleaned_output.json"

    inter_body = {
        "document_id": document_id,
        "validated_output": validated_payload.get("validated_output"),
        "metadata": validated_payload.get("metadata"),
    }
    inter_path.write_text(
        json.dumps(inter_body, indent=2, default=str, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    final_body = {
        "document_id": document_id,
        "row_count": len(final_rows),
        "final_cleaned_output": final_rows,
    }
    final_path.write_text(
        json.dumps(final_body, indent=2, default=str, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    logger.info("Wrote validated output | path=%s", inter_path)
    logger.info("Wrote final cleaned output | path=%s", final_path)
    return {"validated": str(inter_path.resolve()), "final": str(final_path.resolve())}
