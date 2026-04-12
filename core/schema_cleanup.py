"""
Dataset-agnostic schema normalization: semantic deduplication, pruning, numeric coercion,
critical-field inference, adaptive confidence, and cleanup logging.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from core.cleaning import amount_from_value, clean_name, is_valid_date, is_valid_email
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

# Keys we never auto-coerce to numbers (semantic strings / dates handled elsewhere)
_NON_NUMERIC_KEYS = frozenset(
    {
        "person_name",
        "name",
        "email",
        "phone",
        "status",
        "organization",
        "city",
        "location",
        "date",
        "full_name",
        "customer_name",
    }
)


def clean_schema(record: dict[str, Any], semantic_map: dict[str, Any] | None) -> tuple[dict[str, Any], bool]:
    """
    Collapse duplicate semantics, prefer canonical names, prune empty / redundant keys.

    Returns ``(cleaned_record, had_normalization)`` for confidence adjustment.
    """
    semantic_map = semantic_map or {}
    out = dict(record)
    had_norm = False

    if not out.get("person_name") and out.get("full_name"):
        out["person_name"] = clean_name(out.get("full_name"))
        del out["full_name"]
        had_norm = True

    # Promote location → city
    if out.get("location") and not out.get("city"):
        out["city"] = out["location"]
        had_norm = True
    if out.get("location") and out.get("city"):
        out.pop("location", None)
        had_norm = True

    # Single person field
    pn = out.get("person_name") or out.get("name")
    if pn:
        out["person_name"] = pn
        if "name" in out:
            del out["name"]
            had_norm = True
        elif out.get("person_name") != pn:
            had_norm = True

    # Single monetary display field (salary → fold into amount if amount missing)
    amt = out.get("amount")
    if amt is None and out.get("salary") is not None:
        s = amount_from_value(out.get("salary"))
        if s is not None:
            out["amount"] = s
            had_norm = True
    if out.get("amount") is not None and "salary" in out:
        out.pop("salary", None)
        had_norm = True

    redundant = ("full_name", "customer_name", "town", "metro", "wage")
    for rk in redundant:
        if rk in out and rk not in ("person_name", "city", "amount"):
            if (rk in ("full_name", "customer_name") and out.get("person_name")) or (
                rk in ("town", "metro") and out.get("city")
            ):
                del out[rk]
                had_norm = True
            elif rk == "wage" and out.get("amount") is not None:
                del out[rk]
                had_norm = True

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

    # Generic numeric coercion for remaining unknown string numbers
    for k in list(out.keys()):
        if k in _RESERVED_KEYS | _NON_NUMERIC_KEYS:
            continue
        v = out[k]
        if isinstance(v, str) and v.strip():
            if re.fullmatch(r"[\s$€£₹-]*\d[\d,.\s$€£₹%-]*", v.strip()):
                parsed = amount_from_value(v)
                if parsed is not None:
                    out[k] = parsed
                    had_norm = True
                elif not any(c.isalpha() for c in v if c.lower() not in "e"):
                    out[k] = None
                    had_norm = True

    if semantic_map:
        logger.debug("Semantic grouping applied | roles=%s", list(semantic_map.keys())[:20])
    if had_norm:
        logger.debug("Duplicate fields removed / schema normalized")

    return out, had_norm


def infer_critical_fields(records: list[dict[str, Any]]) -> list[str]:
    """
    Infer important columns without dataset types: high fill-rate, id-like names, mostly numeric.
    """
    if not records:
        return []

    n = len(records)
    stats: dict[str, dict[str, Any]] = {}

    for r in records:
        for k, v in r.items():
            if k in _RESERVED_KEYS or k.startswith("_"):
                continue
            if k in ("name",):  # redundant with person_name
                continue
            st = stats.setdefault(k, {"filled": 0, "numeric": 0})
            if v is not None and (not isinstance(v, str) or v.strip()):
                st["filled"] += 1
                if isinstance(v, (int, float)) and not isinstance(v, bool) and v == v:
                    st["numeric"] += 1

    critical: list[str] = []
    for k, st in stats.items():
        nk = normalize_field_name(k)
        fill_ratio = st["filled"] / n
        id_like = nk == "id" or nk.endswith("_id") or nk in ("uuid", "key", "ref", "reference")
        num_ratio = st["numeric"] / st["filled"] if st["filled"] else 0.0
        mostly_numeric = num_ratio >= 0.85 and st["numeric"] > 0
        if id_like or (fill_ratio >= 0.75 and st["filled"] >= max(3, n // 2)) or mostly_numeric:
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
    Returns (confidence, was_adjusted).
    """
    c = 1.0
    adjusted = False

    def sub(x: float) -> None:
        nonlocal c, adjusted
        c -= x
        adjusted = True

    if record.get("email") and not is_valid_email(record.get("email")):
        sub(0.1)
    if record.get("date") is not None and str(record.get("date")).strip():
        if not is_valid_date(record.get("date")):
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
    """False if any value looks numeric-as-string but failed parsing (should have been nulled)."""
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
