"""
Dataset-agnostic schema normalization: pruning, numeric coercion,
critical-field inference, adaptive confidence, and cleanup logging.

All logic driven by value patterns and DatasetProfile — no hardcoded column names.

Safety Rule: Do NOT coerce bare small integer strings (e.g. "7", "10") to floats.
These are likely rank, index, or count values — not monetary amounts.
Only coerce when the value clearly has monetary formatting or is a large number.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from core.cleaning import amount_from_value, is_valid_date, is_valid_email
from core.data_profiler import _column_name_suggests_monetary
from parsers.csv_parser import normalize_field_name

logger = logging.getLogger(__name__)

_RESERVED_KEYS = frozenset(
    {
        "confidence",
        "is_anomaly",
        "is_valid_email",
        "is_valid_date",
        "is_valid_numeric",
    }
)

# Bare small integer: a string that is purely a small number (1–100) with no
# currency symbol, comma-formatting, or decimal. These are rank/index values,
# NOT monetary amounts — do not coerce them to float.
_BARE_SMALL_INT_RE = re.compile(r"^\d{1,3}$")
_CURRENCY_SYMBOL_IN_VALUE_RE = re.compile(r"[$€£₹¥₩₽]")


def _is_bare_small_integer_string(v: str) -> bool:
    """
    Return True if the string value looks like a bare small integer (1–999)
    with no currency symbol or comma-formatting.

    These are rank/index/count values and must NOT be coerced to float
    by schema_cleanup — the profiler decides their type from column context.
    """
    s = v.strip()
    if not _BARE_SMALL_INT_RE.match(s):
        return False
    # Extra safety: no currency symbols anywhere
    if _CURRENCY_SYMBOL_IN_VALUE_RE.search(s):
        return False
    try:
        n = int(s)
        return 0 <= n <= 999
    except ValueError:
        return False


def clean_schema(record: dict[str, Any], semantic_map: dict[str, Any] | None) -> tuple[dict[str, Any], bool]:
    """
    Generic schema cleanup: prune empty fields, coerce obvious numeric strings.

    No fixed field promotions (person_name↔full_name, salary→amount, etc.).
    All field relationships are preserved as-is from the source data.

    Coercion Safety Rule:
    - Do NOT coerce bare small integers ("7", "10", "99") to float.
      These are likely ranks or indices, not monetary amounts.
    - Only coerce strings that clearly have monetary formatting (currency symbol,
      comma-formatted large numbers) or are unambiguously large numeric values.
    """
    semantic_map = semantic_map or {}
    out = dict(record)
    had_norm = False

    # Prune meaningless values
    empty_removed = []
    for k in list(out.keys()):
        if k in _RESERVED_KEYS:
            continue
        v = out[k]
        if v is None:
            empty_removed.append(k)
            del out[k]
            had_norm = True
        elif isinstance(v, str) and not v.strip():
            empty_removed.append(k)
            del out[k]
            had_norm = True

    if empty_removed:
        logger.debug("Pruned empty fields: %s", empty_removed[:15])

    # Selective numeric coercion for string values that look clearly numeric.
    # SAFETY: skip bare small integers — they are likely ranks/indices, not money.
    for k in list(out.keys()):
        if k in _RESERVED_KEYS:
            continue
        v = out[k]
        if isinstance(v, str) and v.strip():
            s = v.strip()
            if re.fullmatch(r"[\s$€£₹-]*\d[\d,.\s$€£₹%-]*", s):
                # RULE: If this is a bare small integer AND the column name
                # does NOT suggest monetary intent, do NOT coerce.
                # But if the name says salary/revenue/cost/etc., always coerce.
                if _is_bare_small_integer_string(s) and not _column_name_suggests_monetary(k):
                    logger.debug(
                        "Coercion skipped for bare small integer string=%r in field=%r",
                        s, k,
                    )
                    continue
                parsed = amount_from_value(s)
                if parsed is not None:
                    out[k] = parsed
                    had_norm = True
                elif not any(c.isalpha() for c in s if c.lower() not in "e"):
                    out[k] = None
                    had_norm = True

    if semantic_map:
        logger.debug("Semantic grouping applied | roles=%s", list(semantic_map.keys())[:20])
    if had_norm:
        logger.debug("Schema normalized")

    return out, had_norm


def infer_critical_fields(records: list[dict[str, Any]]) -> list[str]:
    """
    Infer important columns from statistical properties:
    - high fill-rate
    - near-unique values (candidate keys)
    - mostly numeric

    NO hardcoded field name tokens. Pure statistical inference.
    """
    if not records:
        return []

    n = len(records)
    stats: dict[str, dict[str, Any]] = {}

    for r in records:
        for k, v in r.items():
            if k in _RESERVED_KEYS or k.startswith("_"):
                continue
            st = stats.setdefault(k, {"filled": 0, "numeric": 0, "distinct": set()})
            if v is not None and (not isinstance(v, str) or v.strip()):
                st["filled"] += 1
                st["distinct"].add(str(v).strip().lower())
                if isinstance(v, (int, float)) and not isinstance(v, bool) and v == v:
                    st["numeric"] += 1

    critical: list[str] = []
    for k, st in stats.items():
        fill_ratio = st["filled"] / n if n else 0
        num_ratio = st["numeric"] / st["filled"] if st["filled"] else 0.0
        mostly_numeric = num_ratio >= 0.85 and st["numeric"] > 0
        uniqueness_ratio = len(st["distinct"]) / st["filled"] if st["filled"] else 0.0
        highly_unique = st["filled"] >= max(3, n // 2) and uniqueness_ratio >= 0.90
        high_fill = fill_ratio >= 0.75 and st["filled"] >= max(3, n // 2)

        if highly_unique or high_fill or mostly_numeric:
            critical.append(k)

    out = sorted(set(critical))[:16]
    if out:
        logger.info("Inferred critical fields (dynamic): %s", out)
    return out


def compute_adaptive_confidence(
    record: dict[str, Any],
    critical_fields: list[str],
    *,
    had_schema_normalization: bool,
) -> tuple[float, bool]:
    """
    Start at 1.0; subtract for issues; clamp [0.5, 1.0].
    Uses profile-driven validation, not hardcoded field names.
    """
    c = 1.0
    adjusted = False

    def sub(x: float) -> None:
        nonlocal c, adjusted
        c -= x
        adjusted = True

    # Check validation flags (set dynamically by the profiler)
    if record.get("is_valid_email") is False:
        sub(0.1)
    if record.get("is_valid_date") is False:
        sub(0.1)
    if not record.get("is_valid_numeric", True):
        sub(0.1)

    missing_any_critical = False
    for cf in critical_fields:
        v = record.get(cf)
        if v is None or (isinstance(v, str) and not str(v).strip()):
            missing_any_critical = True
            break
    if missing_any_critical and critical_fields:
        sub(0.2)

    if record.get("is_anomaly"):
        sub(0.15)

    if had_schema_normalization:
        sub(0.05)

    c = max(0.5, min(1.0, c))
    if adjusted:
        logger.debug("Confidence adjusted dynamically | result=%.3f", c)
    return c, adjusted


def validate_row_numeric_aggregate(record: dict[str, Any]) -> bool:
    """False if any value looks numeric-as-string but failed parsing."""
    for k, v in record.items():
        if k in _RESERVED_KEYS:
            continue
        if isinstance(v, str) and v.strip():
            if re.fullmatch(r"[\s$€£₹-]*\d[\d,.\s$€£₹%-]*", v.strip()) and not any(
                c.isalpha() for c in v if c.lower() not in "e"
            ):
                if amount_from_value(v) is None:
                    return False
    return True
