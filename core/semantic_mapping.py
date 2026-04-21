"""
Dataset-agnostic semantic column classification: value-pattern-based detection,
row-level semantic extraction.

Column roles are inferred from DATA PATTERNS, not keyword matching on column names.
The profiler drives all type decisions.
"""

from __future__ import annotations

import logging
from typing import Any

from core.cleaning import amount_from_value
from core.data_profiler import (
    _is_present,
    _looks_like_email,
    _looks_like_phone,
    _looks_like_date,
    _column_name_suggests_monetary,
    profile_column,
)
from parsers.csv_parser import normalize_field_name

logger = logging.getLogger(__name__)

_INVALID_SOURCE_KEY_FRAGMENTS = (
    "is_valid_",
    "is_anomaly",
    "confidence",
    "__",
    "imputed",
    "metadata",
)


def _is_valid_source_column_name(col: str) -> bool:
    nk = normalize_field_name(str(col))
    if not nk:
        return False
    return not any(frag in nk for frag in _INVALID_SOURCE_KEY_FRAGMENTS)


def classify_fields(columns: list[str], sample_rows: list[dict[str, Any]] | None = None) -> dict[str, list[str]]:
    """
    Map semantic roles → list of original column names.

    When sample_rows are provided, classification uses VALUE PATTERNS.
    When not provided, uses lightweight value-agnostic heuristics.
    Each column assigned at most once.
    """
    field_map: dict[str, list[str]] = {}
    used: set[str] = set()

    if sample_rows:
        # Profile-driven classification from actual values
        for col in columns:
            if col is None or not str(col).strip():
                continue
            orig = str(col).strip()
            if orig in used:
                continue
            if not _is_valid_source_column_name(orig):
                continue

            values = [r.get(orig) for r in sample_rows]
            cp = profile_column(orig, values)

            role = _type_to_role(cp.inferred_type, semantic_confidence_high=cp.semantic_confidence_high)
            # Column-name override: if the name suggests monetary intent
            # and values are numeric, force the amount_monetary role.
            if not role and cp.inferred_type in ("numeric", "monetary") and _column_name_suggests_monetary(orig):
                role = "amount_monetary"
            if role:
                field_map.setdefault(role, []).append(orig)
                used.add(orig)
            else:
                logger.debug("No semantic role for column=%r (type=%s)", orig, cp.inferred_type)
    else:
        # Lightweight heuristic without data (preserves backward compat)
        for col in columns:
            if col is None or not str(col).strip():
                continue
            orig = str(col).strip()
            if orig in used:
                continue
            if not _is_valid_source_column_name(orig):
                continue
            used.add(orig)
            # Without data, all columns go unmapped
            logger.debug("Skipped column=%r (no sample data for profiling)", orig)

    nonempty = {k: v for k, v in field_map.items() if v}
    if nonempty:
        logger.info("Using semantic mapping for fields: %s", list(nonempty.keys()))

    unmapped = [str(c).strip() for c in columns if c and str(c).strip() and str(c).strip() not in used]
    if unmapped:
        logger.info("Unmapped columns: %s", unmapped[:20])

    return field_map


def _type_to_role(inferred_type: str, *, semantic_confidence_high: bool = True) -> str | None:
    """
    Map profiler inferred types to semantic roles.

    SAFETY RULE: Only assign a semantic role when confidence is HIGH.
    - "monetary" → "amount_monetary" (requires confirmed currency symbol evidence)
    - "numeric" → None  (generic number; could be rank, score, count — do NOT force money label)
    - Identifier-typed columns with low confidence (rank/index) → None
    """
    if not semantic_confidence_high:
        # Low confidence: keep generic, do not assign a potentially wrong role
        return None

    mapping = {
        "email": "email",
        "phone": "phone",
        "date": "date",
        "monetary": "amount_monetary",
        # "numeric" is intentionally NOT mapped to "amount_monetary".
        # A numeric column with no currency evidence is likely a rank, score, count,
        # quantity, or index — not money. Assigning "amount_monetary" would be WRONG.
        # Rule: be generic but correct, not specific but wrong.
        "text": None,
        "categorical": None,
        "boolean": None,
        "identifier": None,
        "numeric": None,
    }
    return mapping.get(inferred_type)


def dynamic_semantic_map(row: dict[str, Any], field_map: dict[str, Any]) -> dict[str, Any]:
    """First non-empty cell per semantic role (original CSV keys)."""
    out: dict[str, Any] = {}
    for role, cols in field_map.items():
        if cols is None:
            continue
        col_list = cols if isinstance(cols, list) else [cols]
        col_list = list(col_list)
        for c in col_list:
            if not _is_valid_source_column_name(str(c)):
                continue
            if c not in row:
                continue
            v = row.get(c)
            if v is None:
                continue
            if isinstance(v, str) and not v.strip():
                continue
            # Guard: monetary roles must hold numeric-like values
            if role in ("amount_monetary", "salary_comp") and amount_from_value(v) is None:
                continue
            out[role] = v
            break
    return out


def merge_field_maps(
    base: dict[str, list[str]],
    overlay: dict[str, Any] | None,
    *,
    valid_columns: set[str] | None = None,
) -> dict[str, list[str]]:
    """Union column lists per role."""
    out: dict[str, list[str]] = {k: list(v) for k, v in base.items()}
    if not overlay:
        return out
    for key, cols in overlay.items():
        role = str(key)
        if role not in out:
            out[role] = []
        seq = cols if isinstance(cols, list) else [cols]
        for c in seq:
            if c is None:
                continue
            s = str(c).strip()
            if not s:
                continue
            if not _is_valid_source_column_name(s):
                continue
            if valid_columns is not None and s not in valid_columns:
                continue
            if s not in out[role]:
                out[role].append(s)
    return out


def field_map_nonempty(field_map: dict[str, Any]) -> bool:
    return bool(field_map) and any(v for v in field_map.values() if v)


def field_map_needs_ai(field_map: dict[str, Any]) -> bool:
    if not field_map_nonempty(field_map):
        return True
    filled = sum(1 for v in field_map.values() if v)
    return filled < 2
