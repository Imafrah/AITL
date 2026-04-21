"""
Context-Aware Final Cleaning Layer — schema-agnostic, non-destructive, data-driven.

Paradigm: Observe → Infer → Decide → Clean

All decisions derived from DatasetProfile. NO hardcoded column names.
Cleaning strategy adapts to inferred dataset type:
  - Entity: normalize text, validate structured fields, allow safe imputation
  - Transactional: preserve numeric, normalize formats, deduplicate
  - Analytical: NO modification of numeric values, NO imputation, clean noise only

Policy driven by :class:`CleaningConfig` (environment variables).
"""

from __future__ import annotations

import copy
import json
import logging
import os
import re
import statistics
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
from core.data_profiler import (
    ColumnProfile,
    DatasetProfile,
    _INTERNAL_KEYS,
    _PIPELINE_ARTIFACT_KEYS,
    _RESERVED_KEYS,
    _coerce_bool,
    _coerce_number,
    _is_present,
    clean_text_noise,
    compute_median,
    compute_mode,
    detect_field_types,
    detect_numeric_outliers,
    profile_dataset,
)
from parsers.csv_parser import normalize_field_name

logger = logging.getLogger(__name__)

# Keys excluded from missing-rate denominators
_RATIO_EXCLUDED_KEYS = _RESERVED_KEYS | frozenset({"is_outlier"}) | _INTERNAL_KEYS


@dataclass(frozen=True)
class CleaningConfig:
    """
    Env-driven cleaning policy (no dataset-specific logic).

    * ``AITL_CLEAN_MODE``: ``safe`` (default) | ``strict``
    * ``AITL_EMAIL_INVALID_STRATEGY``: ``none`` (default) | ``remove_row`` | ``placeholder``
    * ``AITL_EMAIL_PLACEHOLDER``: placeholder address when strategy is ``placeholder``
    * ``AITL_TEXT_MISSING_PLACEHOLDER``: fill only missing text cells (never IDs/phones)
    * ``AITL_TRACK_IMPUTATION``: ``1`` (default) | ``0``
    * ``AITL_MIN_VALUES_FOR_MEDIAN``: min observed values for median imputation (default ``3``)
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
            "0", "false", "no", "off",
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


# ═══════════════════════════════════════════════════════════════════════════════
# Schema Enforcement
# ═══════════════════════════════════════════════════════════════════════════════

def enforce_schema(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Ensure every record contains all keys seen across the dataset."""
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


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _strip_pipeline_artifacts(row: dict[str, Any]) -> dict[str, Any]:
    return {
        k: v
        for k, v in row.items()
        if k not in _PIPELINE_ARTIFACT_KEYS and not str(k).startswith("__")
    }


def _missing_ratio(record: dict[str, Any], schema_keys: list[str]) -> float:
    denom_keys = [k for k in schema_keys if k not in _RATIO_EXCLUDED_KEYS]
    if not denom_keys:
        return 0.0
    missing = sum(1 for k in denom_keys if not _is_present(record.get(k)))
    return missing / len(denom_keys)


def _compute_null_rate(records: list[dict[str, Any]], schema_keys: list[str]) -> float:
    """Fraction of None/empty cells across entire dataset (excluding reserved keys)."""
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


def _dedupe_by_keys(
    records: list[dict[str, Any]],
    dedup_keys: list[str],
) -> tuple[list[dict[str, Any]], int]:
    """Deduplicate using value-based fingerprinting on the given keys."""
    if not dedup_keys:
        return _dedupe_identical_rows(records)
    seen_map: dict[tuple, dict[str, Any]] = {}
    for r in records:
        sig = tuple(str(r.get(k, "")).strip().lower() for k in dedup_keys)
        conf = float(r.get("confidence", 0.0))
        if sig not in seen_map or conf > float(seen_map[sig].get("confidence", 0.0)):
            seen_map[sig] = r
    deduped = list(seen_map.values())
    return deduped, len(records) - len(deduped)


def _safe_default_for_type(inferred_type: str, *, dataset_type: str = "entity") -> Any:
    """
    Return a safe default value based on inferred column type.

    NON-DESTRUCTIVE PRINCIPLE:
    - Analytical datasets: ALWAYS return None (never fabricate values)
    - Entity/transactional: minimal safe defaults only when caller opts in
    """
    # Analytical data: NEVER fabricate values
    if dataset_type == "analytical":
        return None

    if inferred_type in ("boolean",):
        return False
    if inferred_type in ("numeric", "monetary"):
        return None  # Do NOT replace missing numbers with 0
    if inferred_type in ("phone", "email", "identifier"):
        return None  # Do NOT fill structured types
    if inferred_type in ("date",):
        return None  # Do NOT fabricate dates
    # Text: only fill if explicitly opted in via text_missing_placeholder
    return None


def _schema_key_order(all_keys: Iterable[str], *, include_imputed_slot: bool) -> list[str]:
    keys = list(all_keys)
    content = sorted(
        k for k in keys
        if k not in _RESERVED_KEYS and k not in _INTERNAL_KEYS and not str(k).startswith("__")
    )
    reserved = sorted(k for k in keys if k in _RESERVED_KEYS)
    other = sorted(
        k for k in keys
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


# ═══════════════════════════════════════════════════════════════════════════════
# Context-aware normalization (uses DatasetProfile types, not column names)
# ═══════════════════════════════════════════════════════════════════════════════

def _normalize_value_by_type(
    value: Any,
    col_profile: ColumnProfile,
    dataset_type: str,
) -> Any:
    """
    Normalize a value based on its inferred TYPE, not its column name.

    For analytical datasets: only clean noise, NEVER modify numeric values.
    """
    if not _is_present(value):
        return None

    ct = col_profile.inferred_type

    # Analytical data: preserve all values, only clean string noise
    if dataset_type == "analytical":
        if isinstance(value, str):
            return clean_text_noise(value.strip())
        return value

    # Email: validate format
    if ct == "email":
        s = str(value).strip().lower()
        if is_valid_email(s):
            return s
        return None  # null invalid emails

    # Phone: normalize to digits
    if ct == "phone":
        s = str(value).strip()
        digits = re.sub(r"\D+", "", s)
        if 10 <= len(digits) <= 15:
            return digits
        return None

    # Date: parse to ISO or null
    if ct == "date":
        iso = normalize_date_value(value)
        if iso:
            return iso
        return None

    # Numeric / monetary: coerce to number
    if ct in ("numeric", "monetary"):
        # For transactional data: preserve original, only coerce format
        n = _coerce_number(value)
        if n is not None:
            if n == int(n) and abs(n) < 1e12:
                return int(n)
            return float(n)
        return None

    # Boolean
    if ct == "boolean":
        b = _coerce_bool(value)
        if b is not None:
            return b
        return value

    # Text / categorical / identifier: normalize string
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # Clean noise
        s = clean_text_noise(s)
        # Normalize whitespace
        s = re.sub(r"\s+", " ", s)
        # Title case for text/categorical (not identifiers, not URLs)
        if ct in ("text", "categorical") and "@" not in s and "://" not in s:
            s = s.title()
        return s

    return value


# ═══════════════════════════════════════════════════════════════════════════════
# Validation flags (dynamic, profile-driven)
# ═══════════════════════════════════════════════════════════════════════════════

def _refresh_validation_flags(
    record: dict[str, Any],
    email_cols: set[str] | None = None,
    date_cols: set[str] | None = None,
) -> None:
    """Recompute validation booleans using dynamically detected column sets."""
    e_cols = email_cols or set()
    if e_cols:
        any_email_valid = False
        any_email_present = False
        for ek in e_cols:
            em = record.get(ek)
            if em and str(em).strip():
                any_email_present = True
                if is_valid_email(str(em)):
                    any_email_valid = True
        record["is_valid_email"] = any_email_valid if any_email_present else True
    else:
        record.pop("is_valid_email", None)

    d_cols = date_cols or set()
    if d_cols:
        any_date_invalid = False
        any_date_present = False
        for dk in d_cols:
            dt = record.get(dk)
            if dt is not None and str(dt).strip():
                any_date_present = True
                if not is_valid_date(dt):
                    any_date_invalid = True
        record["is_valid_date"] = not any_date_invalid if any_date_present else True
    else:
        record["is_valid_date"] = True

    record["is_valid_numeric"] = True


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY: run_final_cleaning_layer
# ═══════════════════════════════════════════════════════════════════════════════

def run_final_cleaning_layer(
    records: list[dict[str, Any]],
    *,
    config: CleaningConfig | None = None,
    **kwargs: Any,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Context-aware cleaning pass on a **copy** of rows.

    Cleaning strategy driven by DatasetProfile:
      - Entity: normalize text, validate structured, safe imputation
      - Transactional: preserve numeric, normalize formats, deduplicate
      - Analytical: NO modification, NO imputation, noise removal only
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

    logger.info("Final cleaning started | mode=%s", cfg.clean_mode)

    # Deep copy to avoid mutating originals
    working = copy.deepcopy(records)
    for r in working:
        r.pop("__imputed__", None)

    # Enforce consistent schema
    working = enforce_schema(working)
    schema_keys = list(working[0].keys())

    # ── Measure null rate BEFORE cleaning ──
    null_rate_before = _compute_null_rate(working, schema_keys)

    # ── Profile the dataset (the brain) ──
    profile = profile_dataset(working)
    dataset_type = profile.dataset_type
    logger.info("Dataset type detected: %s", dataset_type)

    # Build type sets for stats and backward compat
    email_cols: set[str] = set()
    date_cols: set[str] = set()
    numeric_cols: set[str] = set()
    text_cols: set[str] = set()
    phone_cols: set[str] = set()
    id_like_cols: set[str] = set()
    boolean_cols: set[str] = set()
    no_impute_cols: set[str] = set()

    for name, cp in profile.columns.items():
        if cp.inferred_type == "email":
            email_cols.add(name)
        elif cp.inferred_type == "date":
            date_cols.add(name)
        elif cp.inferred_type in ("numeric", "monetary"):
            numeric_cols.add(name)
        elif cp.inferred_type == "phone":
            phone_cols.add(name)
        elif cp.inferred_type == "boolean":
            boolean_cols.add(name)
        elif cp.inferred_type == "identifier":
            id_like_cols.add(name)
        else:
            text_cols.add(name)

        if not cp.allows_imputation:
            no_impute_cols.add(name)

    stats["field_types"] = {
        "numeric": sorted(numeric_cols),
        "email": sorted(email_cols),
        "date": sorted(date_cols),
        "text": sorted(text_cols),
        "phone": sorted(phone_cols),
        "identifier": sorted(id_like_cols),
        "boolean": sorted(boolean_cols),
    }
    stats["dataset_type"] = dataset_type

    content_keys = [
        k for k in schema_keys
        if k not in _RESERVED_KEYS and k not in _INTERNAL_KEYS and not str(k).startswith("__")
    ]

    invalid_fixed_cells = 0
    invalid_changed = False

    # ═══════════════════════════════════════════════════════════════════════════
    # PASS 1: Type-aware normalization (driven by DatasetProfile, not names)
    # ═══════════════════════════════════════════════════════════════════════════

    for r in working:
        for k in list(r.keys()):
            if k in _RESERVED_KEYS or k in _INTERNAL_KEYS or str(k).startswith("__"):
                continue
            cp = profile.columns.get(k)
            if not cp:
                continue

            v = r.get(k)
            if not _is_present(v):
                continue

            old_val = v
            new_val = _normalize_value_by_type(v, cp, dataset_type)

            if new_val is None and _is_present(old_val):
                invalid_changed = True
                invalid_fixed_cells += 1
            elif new_val != old_val and _is_present(old_val):
                invalid_fixed_cells += 1

            r[k] = new_val

    if invalid_changed:
        logger.info("Invalid values replaced")

    # ═══════════════════════════════════════════════════════════════════════════
    # PASS 2: Email placeholder (explicit opt-in)
    # ═══════════════════════════════════════════════════════════════════════════

    if cfg.email_invalid_strategy == "placeholder":
        for r in working:
            for k in email_cols:
                if not _is_present(r.get(k)):
                    r[k] = cfg.email_placeholder
                    invalid_fixed_cells += 1
                    if cfg.track_imputation:
                        im = r.setdefault("__imputed__", {})
                        im[k] = "placeholder"

    # ═══════════════════════════════════════════════════════════════════════════
    # PASS 3: Imputation (context-aware, respects dataset type)
    # ═══════════════════════════════════════════════════════════════════════════

    values_filled_cells = 0
    text_ph_cells = 0
    filled_any = False

    # Analytical data: NEVER impute anything
    if dataset_type != "analytical":
        # Numeric imputation (median) — only for columns that allow it
        # Include monetary columns (they are also numeric)
        imputable_numeric = numeric_cols | {n for n, c in profile.columns.items() if c.inferred_type == "monetary"}
        for k in imputable_numeric:
            cp = profile.columns.get(k)
            if not cp or not cp.allows_imputation:
                continue
            if k in no_impute_cols:
                continue

            vals: list[float] = []
            for r in working:
                v = r.get(k)
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    vals.append(float(v))

            if len(vals) < cfg.min_values_for_median:
                continue

            # Strict mode: only impute if null rate < 20%
            if cfg.clean_mode == "strict":
                missing_count = sum(1 for r in working if not _is_present(r.get(k)))
                null_rate = missing_count / len(working) if working else 1.0
                if null_rate >= 0.20:
                    continue

            med = compute_median(vals)
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

    # ═══════════════════════════════════════════════════════════════════════════
    # PASS 4: Row quality filtering (NON-DESTRUCTIVE)
    #
    # ROW DELETION POLICY:
    # - Do NOT delete rows unless ALL meaningful fields are missing or unreadable.
    # - Partial nulls, missing years, and missing optional fields are NOT
    #   reasons to delete a row.
    # - Confidence score alone is NOT a reason to delete a row.
    # - Analytical datasets: NEVER drop rows, NEVER apply strict filtering.
    # ═══════════════════════════════════════════════════════════════════════════

    before_filter = len(working)
    kept: list[dict[str, Any]] = []

    critical_fields = profile.critical_columns
    stats["critical_fields_detected"] = critical_fields

    # ANALYTICAL datasets: NEVER drop rows (preserve all data points)
    if dataset_type == "analytical":
        logger.info("Analytical dataset — row filtering SKIPPED (non-destructive)")
        kept = list(working)
    else:
        for r in working:
            # Only drop rows where ALL meaningful fields are missing.
            # A row with at least one meaningful (non-reserved) field is KEPT.
            has_any_meaningful = False
            for k in content_keys:
                if _is_present(r.get(k)):
                    has_any_meaningful = True
                    break

            if not has_any_meaningful:
                # Completely empty row — safe to drop
                continue

            # Email remove_row strategy is an explicit user opt-in
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

    # ═══════════════════════════════════════════════════════════════════════════
    # PASS 5: Text placeholder fill (only non-ID, non-phone, non-email)
    # ═══════════════════════════════════════════════════════════════════════════

    protected_cols = phone_cols | email_cols | id_like_cols

    for r in working:
        # Text placeholder (safe mode only, non-analytical)
        if cfg.text_missing_placeholder and cfg.clean_mode != "strict" and dataset_type != "analytical":
            for k in text_cols:
                if k in protected_cols:
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

    # Ensure schema consistency
    union_keys: set[str] = set(schema_keys)
    for r in working:
        union_keys |= set(r.keys())
    for r in working:
        for k in union_keys:
            if k not in r:
                r[k] = None

    # ═══════════════════════════════════════════════════════════════════════════
    # PASS 6: Deduplication (dynamic keys)
    # ═══════════════════════════════════════════════════════════════════════════

    working, dup_removed = _dedupe_by_keys(working, profile.dedup_keys)
    stats["duplicate_rows_removed"] = dup_removed

    # ═══════════════════════════════════════════════════════════════════════════
    # PASS 7: Final production shaping
    # ═══════════════════════════════════════════════════════════════════════════

    # Order keys
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

    # Strip pipeline artifacts and fill remaining nulls with safe defaults
    final_content_keys = sorted({
        k for r in working for k in r.keys()
        if k not in _PIPELINE_ARTIFACT_KEYS and not str(k).startswith("__")
    })

    # Re-profile for final type assignment (after cleaning)
    final_profile = profile_dataset(working) if working else DatasetProfile()

    shaped: list[dict[str, Any]] = []
    dropped_for_critical = 0

    for row in working:
        r = _strip_pipeline_artifacts(dict(row))

        # Re-attach __imputed__ if tracking is enabled
        if cfg.track_imputation and row.get("__imputed__"):
            r["__imputed__"] = row["__imputed__"]

        # Normalize boolean columns
        for bk in boolean_cols:
            if bk in r:
                bval = _coerce_bool(r.get(bk))
                if bval is not None:
                    r[bk] = bval

        # ROW DELETION POLICY: Do NOT delete rows with partial data here.
        # PASS 4 already removed completely-empty rows. No further filtering.
        # Analytical datasets: NEVER drop rows in final shaping.

        # NON-DESTRUCTIVE null handling:
        # - Analytical: NEVER fill nulls (preserve original data)
        # - Entity/Transactional: fill only when non-destructive
        if dataset_type != "analytical":
            for k in final_content_keys:
                if not _is_present(r.get(k)):
                    cp = final_profile.columns.get(k) or profile.columns.get(k)
                    ctype = cp.inferred_type if cp else "text"
                    default = _safe_default_for_type(ctype, dataset_type=dataset_type)
                    if default is not None:
                        r[k] = default

        shaped.append(r)

    if dropped_for_critical:
        stats["low_quality_removed"] += dropped_for_critical
    working = shaped

    rows_out = len(working)

    # ── Measure null rate AFTER cleaning ──
    final_schema_keys = list(working[0].keys()) if working else schema_keys
    null_rate_after = _compute_null_rate(working, final_schema_keys)
    content_field_count = len(
        [k for k in final_schema_keys if k not in _RATIO_EXCLUDED_KEYS]
    )

    # ── Compute quality score (0.0–1.0) ──
    q = 1.0
    if null_rate_after > 0:
        q -= min(null_rate_after * 0.5, 0.25)
    if rows_in > 0:
        removal_ratio = (rows_in - rows_out) / rows_in
        q -= min(removal_ratio * 0.3, 0.15)
    if invalid_fixed_cells > 0 and rows_in > 0:
        invalid_ratio = invalid_fixed_cells / max(rows_in * content_field_count, 1)
        q -= min(invalid_ratio * 0.5, 0.20)
    quality_score = round(max(0.0, min(1.0, q)), 4)

    if cfg.clean_mode == "strict":
        stats["missing_values_filled"] = True

    stats["invalid_values_replaced"] = invalid_changed
    stats["rows_out"] = rows_out
    stats["id_like_text_columns"] = sorted(id_like_cols)
    stats["phone_like_columns"] = sorted(phone_cols)
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


# ═══════════════════════════════════════════════════════════════════════════════
# Output writer
# ═══════════════════════════════════════════════════════════════════════════════

def write_cleaning_outputs(
    document_id: str,
    validated_payload: dict[str, Any],
    final_rows: list[dict[str, Any]],
    *,
    cleaning_stats: dict[str, Any] | None = None,
    output_dir: str | os.PathLike[str] | None = None,
) -> dict[str, str]:
    """
    Write ``validated_output.json`` and ``final_cleaned_output.json``.
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
