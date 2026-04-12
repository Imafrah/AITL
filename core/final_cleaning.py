"""
Final cleaning layer — dataset-agnostic, conservative ETL-style repair.

Policy is driven by :class:`CleaningConfig` (environment variables), including
clean mode (``safe`` / ``strict``), email handling (``none`` / ``remove_row`` / ``placeholder``),
optional text sentinels, median imputation thresholds, and optional ``__imputed__`` lineage.
Never bulk-fills phone-like, ID-like, or near-unique identifier columns.

Imputation lineage records the *method* used (``median``, ``placeholder``,
``text_placeholder``) so every filled value is explainable.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from core.cleaning import (
    amount_from_value,
    clean_name,
    is_valid_date,
    is_valid_email,
    is_valid_phone,
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

# Internal / lineage keys — never imputed, excluded from missing-rate denominators.
_INTERNAL_KEYS = frozenset({"__imputed__"})

# Keys used only for QC / scoring — excluded from "missing rate" so rows are not
# penalized for synthetic boolean fields.
_RATIO_EXCLUDED_KEYS = _RESERVED_KEYS | frozenset({"is_outlier"}) | _INTERNAL_KEYS


@dataclass(frozen=True)
class CleaningConfig:
    """
    Env-driven cleaning policy (no dataset-specific table names).

    * ``AITL_CLEAN_MODE``: ``safe`` (default) | ``strict`` — strict uses a lower
      missing-value threshold for row drops.
    * ``AITL_EMAIL_INVALID_STRATEGY``: ``none`` (default) | ``remove_row`` | ``placeholder``
    * ``AITL_EMAIL_PLACEHOLDER``: placeholder address when strategy is ``placeholder``
    * ``AITL_TEXT_MISSING_PLACEHOLDER``: if non-empty, fill **only** missing *text* cells
      (never phones / high-cardinality identifiers).
    * ``AITL_TRACK_IMPUTATION``: ``1`` (default) | ``0`` — add per-row ``__imputed__`` map.
    * ``AITL_MIN_VALUES_FOR_MEDIAN``: minimum count of observed numeric values required
      before median imputation is allowed (default ``3``).
    """

    clean_mode: str
    email_invalid_strategy: str
    email_placeholder: str
    text_missing_placeholder: str | None
    track_imputation: bool
    min_values_for_median: int

    @staticmethod
    def from_env() -> CleaningConfig:
        mode = os.getenv("AITL_CLEAN_MODE", "safe").strip().lower()
        if mode not in ("safe", "strict"):
            mode = "safe"
        estrat = os.getenv("AITL_EMAIL_INVALID_STRATEGY", "none").strip().lower()
        if estrat not in ("none", "remove_row", "placeholder"):
            estrat = "none"
        ph = (os.getenv("AITL_EMAIL_PLACEHOLDER") or "unknown@example.invalid").strip()
        if not is_valid_email(ph):
            ph = "unknown@example.invalid"
        txt_ph = (os.getenv("AITL_TEXT_MISSING_PLACEHOLDER") or "").strip()
        txt_out: str | None = txt_ph if txt_ph else None
        track = os.getenv("AITL_TRACK_IMPUTATION", "1").strip().lower() not in (
            "0",
            "false",
            "no",
            "off",
        )
        try:
            min_med = int(os.getenv("AITL_MIN_VALUES_FOR_MEDIAN", "3"))
        except ValueError:
            min_med = 3
        min_med = max(2, min_med)
        return CleaningConfig(
            clean_mode=mode,
            email_invalid_strategy=estrat,
            email_placeholder=ph,
            text_missing_placeholder=txt_out,
            track_imputation=track,
            min_values_for_median=min_med,
        )


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

    Returns ``{"numeric": set(...), "email": set(...), "date": set(...), "text": set(...)}``.
    Each non-reserved key appears in exactly one bucket.
    """
    out: dict[str, set[str]] = {"numeric": set(), "email": set(), "date": set(), "text": set()}
    if not records:
        return out

    keys = [
        k
        for k in records[0].keys()
        if k not in _RESERVED_KEYS
        and k not in _INTERNAL_KEYS
        and not str(k).startswith("__")
    ]
    n = len(records)

    for k in keys:
        filled = 0
        str_vals: list[str] = []
        at_like = 0
        valid_email_n = 0
        num_ok = 0
        date_ok = 0

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
                # Check if the string parses as a date
                if is_valid_date(sv):
                    date_ok += 1
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
        date_ratio = date_ok / filled

        # Email: many cells look like addresses (``@`` + TLD) or mostly valid emails
        if email_ratio >= 0.35 or valid_email_ratio >= 0.25:
            out["email"].add(k)
            continue

        # Date: ≥40% of filled values parse as dates AND the column key hints at temporal
        # data OR ≥60% parse as dates regardless of name. Never classify a mostly-numeric
        # column as date (pure integers like 2024 can parse as dates).
        nk = normalize_field_name(k)
        date_name_hint = any(
            x in nk for x in ("date", "dob", "birth", "timestamp", "created", "updated", "joined", "hired")
        )
        if num_ratio < 0.5 and (
            (date_ratio >= 0.40 and date_name_hint) or date_ratio >= 0.60
        ):
            out["date"].add(k)
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


def _detect_phone_like_columns(records: list[dict[str, Any]], keys: Iterable[str]) -> set[str]:
    """Columns whose values look like phone numbers — never bulk-imputed or text-filled."""
    out: set[str] = set()
    for k in keys:
        filled: list[str] = []
        for r in records:
            v = r.get(k)
            if not _is_present(v):
                continue
            filled.append(str(v).strip())
        if not filled:
            continue
        hit = 0
        for v in filled:
            if is_valid_phone(v):
                hit += 1
                continue
            digits = re.sub(r"\D+", "", v)
            if 10 <= len(digits) <= 15 and len(digits) >= len(v) * 0.45:
                hit += 1
        if hit / len(filled) >= 0.5:
            out.add(k)
    return out


def _detect_id_like_text_columns(
    records: list[dict[str, Any]], text_keys: Iterable[str]
) -> set[str]:
    """
    Text columns that are identifier-like (UUID, serial, reference, code) or
    near-unique — never filled with text placeholders. Detection is purely
    pattern-based (column name heuristics + cardinality), no dataset-specific rules.
    """
    _ID_FRAGMENTS = (
        "id", "uuid", "ref", "reference", "key", "code", "serial",
        "number", "num", "no", "identifier", "index", "idx", "ticket",
        "order", "account", "invoice", "sku", "barcode", "ssn", "passport",
    )
    # Not "phone_number" — those are handled by _detect_phone_like_columns.
    _EXCLUDE_FRAGMENTS = ("phone", "tel", "mobile", "cell", "fax")
    out: set[str] = set()
    for k in text_keys:
        nk = normalize_field_name(k)
        if not nk:
            continue
        # Skip phone-like column names
        if any(x in nk for x in _EXCLUDE_FRAGMENTS):
            continue
        # Name-based: key contains an ID-like fragment
        name_hit = any(x in nk for x in _ID_FRAGMENTS)
        # Cardinality-based: ≥90% unique values when enough data
        filled_vals: list[str] = []
        for r in records:
            v = r.get(k)
            if _is_present(v):
                filled_vals.append(str(v).strip().lower())
        if not filled_vals:
            continue
        distinct_ratio = len(set(filled_vals)) / len(filled_vals) if filled_vals else 0
        high_cardinality = len(filled_vals) >= 5 and distinct_ratio >= 0.90
        if name_hit or high_cardinality:
            out.add(k)
    return out


def _should_skip_numeric_bulk_impute(records: list[dict[str, Any]], k: str) -> bool:
    """
    Identifier-like / near-unique numeric columns: do not fabricate repeated values.
    """
    vals: list[float] = []
    for r in records:
        v = r.get(k)
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            vals.append(float(v))
    n = len(vals)
    if n < 2:
        return True
    distinct = len({round(v, 6) for v in vals})
    if n <= 24 and distinct >= n - 1:
        return True
    if n >= 5 and distinct / n >= 0.92:
        return True
    return False


def _schema_key_order(all_keys: Iterable[str], *, include_imputed_slot: bool) -> list[str]:
    keys = list(all_keys)
    content = sorted(
        k
        for k in keys
        if k not in _RESERVED_KEYS and k not in _INTERNAL_KEYS and not str(k).startswith("__")
    )
    reserved = sorted(k for k in keys if k in _RESERVED_KEYS)
    other = sorted(
        k
        for k in keys
        if k not in content and k not in reserved and k not in _INTERNAL_KEYS
    )
    out = content + reserved + other
    if include_imputed_slot and "__imputed__" not in out:
        out.append("__imputed__")
    return out


def _reorder_record_keys(row: dict[str, Any], key_order: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k in key_order:
        if k in row:
            out[k] = row[k]
    for k, v in row.items():
        if k not in out:
            out[k] = v
    return out


def _median(vals: list[float]) -> float | None:
    if not vals:
        return None
    s = sorted(vals)
    m = len(s) // 2
    if len(s) % 2:
        return float(s[m])
    return float(s[m - 1] + s[m]) / 2.0


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


def _refresh_validation_flags(
    record: dict[str, Any],
    email_cols: set[str] | None = None,
    date_cols: set[str] | None = None,
) -> None:
    """Recompute validation booleans using dynamically detected column sets."""
    # Email validation — check ALL detected email columns, not just "email"
    e_cols = email_cols or {"email"}
    any_email_valid = False
    any_email_present = False
    for ek in e_cols:
        em = record.get(ek)
        if em and str(em).strip():
            any_email_present = True
            if is_valid_email(str(em)):
                any_email_valid = True
    record["is_valid_email"] = any_email_valid if any_email_present else True

    # Date validation — check ALL detected date columns, not just "date"
    d_cols = date_cols or {"date"}
    any_date_invalid = False
    any_date_present = False
    for dk in d_cols:
        dt = record.get(dk)
        if dt is not None and str(dt).strip():
            any_date_present = True
            if not is_valid_date(dt):
                any_date_invalid = True
    record["is_valid_date"] = not any_date_invalid if any_date_present else True

    record["is_valid_numeric"] = validate_row_numeric_aggregate(record)


def _missing_ratio(record: dict[str, Any], schema_keys: list[str]) -> float:
    denom_keys = [k for k in schema_keys if k not in _RATIO_EXCLUDED_KEYS]
    if not denom_keys:
        return 0.0
    missing = sum(1 for k in denom_keys if not _is_present(record.get(k)))
    return missing / len(denom_keys)


def _compute_null_rate(records: list[dict[str, Any]], schema_keys: list[str]) -> float:
    """Fraction of ``None``/empty cells across the entire dataset (excluding reserved keys)."""
    denom_keys = [k for k in schema_keys if k not in _RATIO_EXCLUDED_KEYS]
    if not denom_keys or not records:
        return 0.0
    total = len(denom_keys) * len(records)
    missing = sum(
        1
        for r in records
        for k in denom_keys
        if not _is_present(r.get(k))
    )
    return round(missing / total, 6) if total else 0.0


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
    config: CleaningConfig | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Production pass on a **copy** of rows: conservative imputation, optional email policy,
    no synthetic IDs/phones, optional ``__imputed__`` lineage with method tags.
    ``config`` defaults to :meth:`CleaningConfig.from_env`.
    """
    cfg = config or CleaningConfig.from_env()
    rows_in = len(records)
    stats: dict[str, Any] = {
        "rows_in": rows_in,
        "rows_out": 0,
        "clean_mode": cfg.clean_mode,
        "email_invalid_strategy": cfg.email_invalid_strategy,
        "invalid_values_replaced": False,
        "missing_values_filled": False,
        "low_quality_removed": 0,
        "duplicate_rows_removed": 0,
        "field_types": {"numeric": [], "email": [], "date": [], "text": []},
        "cleaning_summary": {
            "rows_removed": 0,
            "values_filled": 0,
            "invalid_values_fixed": 0,
            "null_rate_before": 0.0,
            "null_rate_after": 0.0,
            "schema_field_count": 0,
            "quality_score": 1.0,
        },
    }
    if not records:
        return [], stats

    logger.info("Final cleaning started")

    working = copy.deepcopy(records)
    for r in working:
        r.pop("__imputed__", None)

    working = enforce_schema(working)
    logger.info("Schema enforced")

    schema_keys = list(working[0].keys())

    # ── Measure null rate BEFORE cleaning ──
    null_rate_before = _compute_null_rate(working, schema_keys)

    field_types = detect_field_types(working)
    stats["field_types"] = {
        "numeric": sorted(field_types["numeric"]),
        "email": sorted(field_types["email"]),
        "date": sorted(field_types.get("date", set())),
        "text": sorted(field_types["text"]),
    }
    numeric_cols = set(field_types["numeric"])
    email_cols = set(field_types["email"])
    date_cols = set(field_types.get("date", set()))
    text_cols = set(field_types["text"])

    content_keys = [
        k
        for k in schema_keys
        if k not in _RESERVED_KEYS and k not in _INTERNAL_KEYS and not str(k).startswith("__")
    ]
    phone_like = _detect_phone_like_columns(working, content_keys)
    id_like_text = _detect_id_like_text_columns(working, text_cols)

    # Digit-heavy strings (phones) must not be classified or coerced as plain numbers.
    for k in phone_like & numeric_cols:
        numeric_cols.remove(k)
        text_cols.add(k)
    # Date columns must not be coerced as numbers
    for k in date_cols & numeric_cols:
        numeric_cols.remove(k)

    invalid_fixed_cells = 0
    invalid_changed = False

    for r in working:
        for k in list(r.keys()):
            if k in _RESERVED_KEYS or k in _INTERNAL_KEYS or str(k).startswith("__"):
                continue
            v = r.get(k)
            if k in email_cols and v is not None and str(v).strip():
                if not is_valid_email(str(v)):
                    r[k] = None
                    invalid_changed = True
                    if cfg.email_invalid_strategy != "placeholder":
                        invalid_fixed_cells += 1
            elif k in date_cols:
                # Normalize valid dates to ISO, null out unparseable ones
                if v is not None and str(v).strip():
                    iso = normalize_date_value(v)
                    if iso:
                        if str(v).strip() != iso:
                            invalid_fixed_cells += 1
                        r[k] = iso
                    else:
                        r[k] = None
                        invalid_changed = True
                        invalid_fixed_cells += 1
                else:
                    r[k] = None
            elif k in numeric_cols:
                if v is None or (isinstance(v, str) and not str(v).strip()):
                    r[k] = None
                else:
                    before = r.get(k)
                    coerced = _coerce_number(v)
                    if coerced is None and _is_present(v):
                        invalid_changed = True
                        invalid_fixed_cells += 1
                    elif coerced != before and _is_present(before):
                        invalid_fixed_cells += 1
                    r[k] = coerced

    if invalid_changed:
        logger.info("Invalid values replaced")

    skip_numeric_impute = {k for k in numeric_cols if _should_skip_numeric_bulk_impute(working, k)}
    skip_numeric_impute |= phone_like & numeric_cols

    # Email placeholder (explicit opt-in — documented as synthetic marker).
    if cfg.email_invalid_strategy == "placeholder":
        for r in working:
            for k in email_cols:
                if not _is_present(r.get(k)):
                    r[k] = cfg.email_placeholder
                    invalid_fixed_cells += 1
                    if cfg.track_imputation:
                        im = r.setdefault("__imputed__", {})
                        im[k] = "placeholder"

    values_filled_cells = 0
    text_ph_cells = 0
    filled_any = False

    for k in numeric_cols:
        if k in skip_numeric_impute:
            continue
        vals: list[float] = []
        for r in working:
            v = r.get(k)
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                vals.append(float(v))
        if len(vals) < cfg.min_values_for_median:
            continue
        med = _median(vals)
        if med is None:
            continue
        fill_value: float | int = int(med) if med == int(med) and abs(med) < 1e12 else float(med)
        for r in working:
            if r.get(k) is None:
                r[k] = fill_value
                filled_any = True
                values_filled_cells += 1
                if cfg.track_imputation:
                    im = r.setdefault("__imputed__", {})
                    im[k] = "median"

    # ── Row quality filtering — mode-aware ──
    # SAFE:   keep ALL rows; only replace invalid values (already done above)
    # STRICT: aggressively remove rows with null critical fields, invalid
    #         emails, or missing numeric data
    before_filter = len(working)
    kept: list[dict[str, Any]] = []

    if cfg.clean_mode == "strict":
        # Detect critical fields dynamically (high fill-rate, id-like, numeric-heavy)
        critical_fields = infer_critical_fields(working)
        stats["critical_fields_detected"] = critical_fields
        logger.info("Strict mode | critical fields detected: %s", critical_fields)

        for r in working:
            # 1. General missing-ratio filter (tight threshold)
            if _missing_ratio(r, schema_keys) > 0.25:
                continue

            # 2. Critical fields must not be null
            critical_missing = False
            for cf in critical_fields:
                if not _is_present(r.get(cf)):
                    critical_missing = True
                    break
            if critical_missing:
                continue

            # 3. Email columns must be valid (strict rejects all bad emails)
            email_bad = False
            for ek in email_cols:
                ev = r.get(ek)
                if not _is_present(ev) or not is_valid_email(str(ev)):
                    email_bad = True
                    break
            if email_cols and email_bad:
                continue

            # 4. Numeric fields — at least half must be present
            if numeric_cols:
                num_present = sum(1 for nk in numeric_cols if _is_present(r.get(nk)))
                if num_present < max(1, len(numeric_cols) // 2):
                    continue

            kept.append(r)
    else:
        # SAFE mode: keep ALL rows — no quality-based removal
        stats["critical_fields_detected"] = []
        # Only honour explicit email_invalid_strategy="remove_row" if set
        for r in working:
            if cfg.email_invalid_strategy == "remove_row":
                drop = False
                for ek in email_cols:
                    if not _is_present(r.get(ek)) or not is_valid_email(str(r.get(ek))):
                        drop = True
                        break
                if drop:
                    continue
            kept.append(r)

    removed = before_filter - len(kept)
    if removed:
        logger.info("%s mode | rows removed: %d", cfg.clean_mode.upper(), removed)
    stats["low_quality_removed"] = removed

    working = kept

    for r in working:
        _normalize_strings_inplace(r)
        if cfg.text_missing_placeholder and text_cols:
            for k in text_cols:
                # Never fill phone-like, email, or ID-like text columns with placeholders
                if k in phone_like | email_cols | id_like_text:
                    continue
                if not _is_present(r.get(k)):
                    r[k] = cfg.text_missing_placeholder
                    text_ph_cells += 1
                    values_filled_cells += 1
                    if cfg.track_imputation:
                        im = r.setdefault("__imputed__", {})
                        im[k] = "text_placeholder"
        _refresh_validation_flags(r, email_cols=email_cols, date_cols=date_cols)
        r["is_anomaly"] = False

    if filled_any or text_ph_cells:
        logger.info("Missing values filled")
    stats["missing_values_filled"] = bool(filled_any or text_ph_cells)

    union_keys: set[str] = set(schema_keys)
    for r in working:
        union_keys |= set(r.keys())
    for r in working:
        for k in union_keys:
            if k not in r:
                r[k] = None

    working, dup_removed = _dedupe_identical_rows(working)
    stats["duplicate_rows_removed"] = dup_removed

    key_order = _schema_key_order(union_keys, include_imputed_slot=cfg.track_imputation)
    if cfg.track_imputation:
        for r in working:
            if not r.get("__imputed__"):
                r.pop("__imputed__", None)
        working = [_reorder_record_keys(r, key_order) for r in working]
    else:
        for r in working:
            r.pop("__imputed__", None)
        working = [_reorder_record_keys(r, [k for k in key_order if k != "__imputed__"]) for r in working]

    rows_out = len(working)

    # ── Measure null rate AFTER cleaning ──
    final_schema_keys = list(working[0].keys()) if working else schema_keys
    null_rate_after = _compute_null_rate(working, final_schema_keys)
    content_field_count = len(
        [k for k in final_schema_keys if k not in _RATIO_EXCLUDED_KEYS]
    )

    # ── Compute quality score (0.0–1.0) ──
    # Penalize for: remaining nulls, removed rows, invalid values found
    q = 1.0
    if null_rate_after > 0:
        q -= min(null_rate_after * 0.5, 0.25)  # up to -0.25 for nulls
    if rows_in > 0:
        removal_ratio = (rows_in - rows_out) / rows_in
        q -= min(removal_ratio * 0.3, 0.15)  # up to -0.15 for row removal
    if invalid_fixed_cells > 0 and rows_in > 0:
        invalid_ratio = invalid_fixed_cells / max(rows_in * content_field_count, 1)
        q -= min(invalid_ratio * 0.5, 0.20)  # up to -0.20 for invalids
    quality_score = round(max(0.0, min(1.0, q)), 4)

    stats["invalid_values_replaced"] = invalid_changed
    stats["rows_out"] = rows_out
    stats["id_like_text_columns"] = sorted(id_like_text)
    stats["phone_like_columns"] = sorted(phone_like)
    stats["cleaning_summary"] = {
        "rows_removed": rows_in - rows_out,
        "values_filled": values_filled_cells,
        "invalid_values_fixed": invalid_fixed_cells,
        "null_rate_before": null_rate_before,
        "null_rate_after": null_rate_after,
        "schema_field_count": content_field_count,
        "quality_score": quality_score,
    }
    return working, stats


def write_cleaning_outputs(
    document_id: str,
    validated_payload: dict[str, Any],
    final_rows: list[dict[str, Any]],
    *,
    cleaning_stats: dict[str, Any] | None = None,
    output_dir: str | os.PathLike[str] | None = None,
) -> dict[str, str]:
    """
    Write ``validated_output.json`` (flags + anomalies) and
    ``final_cleaned_output.json`` (production rows + cleaning summary).
    Returns paths written.
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
    final_body: dict[str, Any] = {
        "document_id": document_id,
        "row_count": len(final_rows),
        "final_cleaned_output": final_rows,
    }
    # Include cleaning summary so the output file is self-contained and analysis-ready
    if cleaning_stats:
        final_body["cleaning_summary"] = cleaning_stats.get("cleaning_summary", {})
        final_body["field_types"] = cleaning_stats.get("field_types", {})
    final_path.write_text(
        json.dumps(final_body, indent=2, default=str, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    logger.info("Wrote validated output | path=%s", inter_path)
    logger.info("Wrote final cleaned output | path=%s", final_path)
    return {"validated": str(inter_path.resolve()), "final": str(final_path.resolve())}
