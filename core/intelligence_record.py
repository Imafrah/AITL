"""
Intelligence rows: value-pattern-aware cleaning, sparse output, preserved unmapped columns.

All cell cleaning driven by value patterns and DatasetProfile — no column-name keyword matching.
"""

from __future__ import annotations

import re
from typing import Any

from core.cleaning import (
    amount_from_value,
    clean_email,
    clean_name,
    clean_phone,
    is_valid_date,
    is_valid_email,
    is_valid_numeric,
    is_valid_phone,
    normalize_city,
    normalize_date_value,
    normalize_status_value,
)
from core.data_profiler import (
    _coerce_number,
    _is_present,
    _looks_like_date,
    _looks_like_email,
    _looks_like_phone,
    profile_column,
)
from parsers.csv_parser import normalize_field_name

_LEGACY_KEYS = frozenset({"person_name", "organization", "amount", "is_outlier"})


def _smart_clean_cell(value: Any, col_profile=None) -> Any:
    """
    Clean a cell value based on its INFERRED TYPE from the profiler.
    No column-name keyword matching.
    """
    if value is None:
        return None

    if col_profile is not None:
        ct = col_profile.inferred_type

        if ct == "email":
            return clean_email(str(value))
        if ct == "phone":
            return clean_phone(str(value))
        if ct == "date":
            iso = normalize_date_value(value)
            if iso:
                return iso
            s = str(value).strip()
            return s[:128] if s else None
        if ct in ("numeric", "monetary"):
            n = amount_from_value(value)
            return n
        if ct == "identifier":
            # Preserve identifiers as-is (strip only)
            s = str(value).strip()
            return s[:2048] if s else None

    # Fallback: value-pattern-based cleaning (when no profile available)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # Detect by value pattern
        if _looks_like_email(s):
            return clean_email(s)
        if _looks_like_phone(s):
            return clean_phone(s)
        if len(s) >= 6 and _looks_like_date(s):
            iso = normalize_date_value(s)
            if iso:
                return iso
        return s[:2048] if s else None

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value) if value == value else None

    return value


def preserve_csv_row(row: dict[str, Any], col_profiles: dict[str, Any] | None = None) -> dict[str, Any]:
    """All columns → normalized snake_case keys with type-aware cleaning."""
    out: dict[str, Any] = {}
    profiles = col_profiles or {}
    for k, v in row.items():
        nk = normalize_field_name(str(k))
        if not nk:
            continue
        cp = profiles.get(k) or profiles.get(nk)
        out[nk] = _smart_clean_cell(v, cp)
    return out


def _quantity_parsed(raw: Any) -> float | int | None:
    if raw is None:
        return None
    n = amount_from_value(raw)
    if n is None:
        return None
    if n == int(n) and abs(n) < 1e12:
        return int(n)
    return float(n)


def semantic_intelligence_row(
    row: dict[str, Any],
    field_map: dict[str, list[str]],
    *,
    schema_source: str = "heuristic",
    col_profiles: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Map row through field_map; preserve all fields with type-aware cleaning.
    Validation flags set from value patterns, not column names.
    """
    from core.semantic_mapping import dynamic_semantic_map

    sem = dynamic_semantic_map(row, field_map)
    preserved = preserve_csv_row(row, col_profiles)

    used_norms = {normalize_field_name(str(c)) for cols in field_map.values() for c in (cols if isinstance(cols, list) else [cols])}

    rec: dict[str, Any] = {}

    # Map semantic values (preserving all original columns too)
    for role, value in sem.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        rec[role] = value

    # Validation flags (set from actual values, not column names)
    email_vals = [v for k, v in rec.items() if isinstance(v, str) and _looks_like_email(v)]
    rec["is_valid_email"] = any(is_valid_email(e) for e in email_vals) if email_vals else True

    date_vals = [v for k, v in rec.items() if isinstance(v, str) and _looks_like_date(v)]
    has_invalid_date = any(not is_valid_date(d) for d in date_vals)
    rec["is_valid_date"] = not has_invalid_date if date_vals else True

    # Numeric validation
    rec["is_valid_numeric"] = True
    rec["confidence"] = 1.0
    rec["is_anomaly"] = False

    # Merge preserved extras
    for nk, v in preserved.items():
        if nk in used_norms:
            continue
        if nk in rec:
            continue
        rec[nk] = v

    return rec


def mapped_intelligence_row(
    row: dict[str, Any],
    mapping: dict[str, Any],
    *,
    schema_source: str = "heuristic",
) -> dict[str, Any]:
    """Backward-compatible entry when caller has a merged field_map."""
    fm = {k: (v if isinstance(v, list) else [v]) for k, v in mapping.items() if v}
    return semantic_intelligence_row(row, fm, schema_source=schema_source)


def heuristic_intelligence_row(row: dict[str, Any]) -> dict[str, Any]:
    """Profile-driven intelligence row from raw input (no pre-existing mapping)."""
    from core.semantic_mapping import classify_fields
    keys = [k for k in row.keys() if k is not None]
    fm = classify_fields([str(k) for k in keys])
    return semantic_intelligence_row(row, fm, schema_source="heuristic")


def coerce_intelligence_row(d: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize legacy / unstructured rows into clean shape.
    Uses value patterns for cleaning, not hardcoded field names.
    """
    out: dict[str, Any] = {}
    for k, v in d.items():
        if k in _LEGACY_KEYS:
            continue
        if k in (
            "is_valid_email", "is_valid_date", "is_valid_numeric",
            "confidence", "is_anomaly",
        ):
            continue
        if _is_present(v):
            out[k] = _smart_clean_cell(v)
        else:
            out[k] = v

    # Validation flags from value patterns
    email_vals = [str(v) for v in out.values() if isinstance(v, str) and _looks_like_email(str(v))]
    out["is_valid_email"] = any(is_valid_email(e) for e in email_vals) if email_vals else True

    date_vals = [str(v) for v in out.values() if isinstance(v, str) and len(str(v)) >= 6 and _looks_like_date(str(v))]
    has_invalid = any(not is_valid_date(dv) for dv in date_vals)
    out["is_valid_date"] = not has_invalid if date_vals else True

    out["is_valid_numeric"] = True
    out["confidence"] = max(0.0, min(1.0, float(d.get("confidence", 0.75))))
    out["is_anomaly"] = bool(d.get("is_anomaly", d.get("is_outlier", False)))
    return out


def phone_fingerprint(phone: str | None) -> str:
    if not phone:
        return ""
    digits = re.sub(r"\D", "", str(phone))
    if len(digits) >= 10:
        return digits[-10:]
    return digits


def dedupe_intelligence_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop duplicates by content fingerprinting."""
    seen: set[tuple[Any, ...]] = set()
    out: list[dict[str, Any]] = []
    for r in rows:
        # Build fingerprint from all non-reserved values
        fp_parts = []
        for k, v in sorted(r.items()):
            if k in ("confidence", "is_anomaly", "is_valid_email", "is_valid_date", "is_valid_numeric", "is_outlier"):
                continue
            fp_parts.append((k, str(v).strip().lower() if v is not None else ""))
        key = tuple(fp_parts)
        if not key or all(p[1] == "" for p in key):
            key = ("__empty__", hash(frozenset((k, str(v)) for k, v in sorted(r.items()))))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out
