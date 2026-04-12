"""
Generic column → semantic role inference from header text only (no dataset-specific parsers).
Works with dynamic_map_row + build_clean_row for any CSV shape.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from core.cleaning import build_clean_row
from parsers.csv_parser import dynamic_map_row, normalize_field_name, safe_float
from post_processor.processor import parse_date

logger = logging.getLogger(__name__)

# Role match order: first match wins (put narrower / higher-signal roles first).
_ROLE_PRIORITY: tuple[tuple[str, frozenset[str]], ...] = (
    ("currency", frozenset({"currency", "curr", "iso", "fx", "money_unit"})),
    (
        "amount",
        frozenset(
            {
                "amount",
                "price",
                "cost",
                "total",
                "subtotal",
                "tax",
                "fee",
                "payment",
                "balance",
                "salary",
                "wage",
                "revenue",
                "discount",
                "qty",
                "quantity",
                "msrp",
                "rate",
                "net",
                "gross",
                "sum",
                "paid",
                "due",
                "value",
            }
        ),
    ),
    (
        "date",
        frozenset(
            {
                "date",
                "time",
                "timestamp",
                "day",
                "month",
                "year",
                "period",
                "created",
                "updated",
                "start",
                "end",
                "dob",
                "birth",
            }
        ),
    ),
    (
        "person_name",
        frozenset(
            {
                "name",
                "person",
                "user",
                "customer",
                "client",
                "buyer",
                "payer",
                "employee",
                "member",
                "owner",
                "author",
                "patient",
                "student",
                "teacher",
                "contact",
            }
        ),
    ),
    (
        "organization",
        frozenset(
            {
                "company",
                "org",
                "vendor",
                "supplier",
                "merchant",
                "seller",
                "store",
                "brand",
                "bank",
                "department",
                "dept",
                "division",
                "team",
                "unit",
                "employer",
                "agency",
            }
        ),
    ),
)


def _header_matches_role(norm_header: str, keywords: frozenset[str]) -> bool:
    if not norm_header:
        return False
    for kw in keywords:
        if kw == norm_header:
            return True
        if len(kw) >= 3 and kw in norm_header:
            return True
        if norm_header.startswith(kw + "_") or norm_header.endswith("_" + kw):
            return True
    return False


def infer_mapping_from_columns(columns: list[str]) -> dict[str, list[str]]:
    """
    Build semantic mapping: role -> [original column names].
    Each column is assigned at most one role (first matching role in priority).
    """
    mapping: dict[str, list[str]] = {}
    used_cols: set[str] = set()

    for col in columns:
        if not col or col in used_cols:
            continue
        nk = normalize_field_name(str(col))
        if not nk:
            continue
        for role, kws in _ROLE_PRIORITY:
            if _header_matches_role(nk, kws):
                mapping.setdefault(role, []).append(col)
                used_cols.add(col)
                break

    return mapping


def heuristic_row_without_mapping(row: dict[str, Any]) -> dict[str, Any]:
    """
    When no column mapping exists, scan cell values for a plausible name / amount / date.
    """
    texts: list[str] = []
    nums: list[float] = []
    dates: list[str] = []

    for _k, v in row.items():
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        iso = parse_date(s)
        if iso:
            dates.append(iso)
            continue
        n = safe_float(s)
        if n is not None and n == n:
            if n == int(n) and 1900 <= int(n) <= 2100 and len(re.sub(r"[^\d]", "", s)) <= 4:
                continue
            nums.append(float(n))
            continue
        if len(s) >= 2:
            texts.append(s)

    return build_clean_row(
        texts[0] if texts else None,
        None,
        nums[0] if nums else None,
        dates[0] if dates else None,
        0.55,
    )


def mapping_to_universal_row(
    row: dict[str, Any],
    mapping: dict[str, Any],
    *,
    confidence: float = 0.82,
    schema_source: str = "inferred",
) -> dict[str, Any]:
    """Apply dynamic_map_row + universal cleaning (single code path for mapped CSV)."""
    m = dynamic_map_row(row, mapping)
    base_conf = confidence
    if schema_source == "ai":
        base_conf = max(base_conf, 0.86)
    elif schema_source == "heuristic":
        base_conf = min(base_conf, 0.78)

    return build_clean_row(
        m.get("person_name"),
        m.get("organization"),
        m.get("amount"),
        m.get("date"),
        base_conf,
    )


def mapping_is_non_empty(mapping: dict[str, Any]) -> bool:
    return bool(mapping) and any(v for v in mapping.values() if v)
