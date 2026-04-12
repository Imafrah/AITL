"""
Generic column → semantic role inference from header text only (no dataset-specific parsers).
Works with dynamic_map_row + intelligence records for any CSV shape.
"""

from __future__ import annotations

import logging
from typing import Any

from core.intelligence_record import heuristic_intelligence_row, mapped_intelligence_row
from parsers.csv_parser import normalize_field_name

logger = logging.getLogger(__name__)

# Role match order: first match wins (put narrower / higher-signal roles first).
_ROLE_PRIORITY: tuple[tuple[str, frozenset[str]], ...] = (
    ("currency", frozenset({"currency", "curr", "iso", "fx", "money_unit"})),
    ("email", frozenset({"email", "e_mail", "mail", "mailbox"})),
    ("phone", frozenset({"phone", "tel", "mobile", "cell", "fax", "whatsapp"})),
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
        "city",
        frozenset(
            {
                "city",
                "town",
                "municipality",
                "metro",
                "region",
                "state",
                "province",
                "zip",
                "postal",
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
    """When no column mapping exists, preserve all fields and infer slots from values."""
    return heuristic_intelligence_row(row)


def mapping_to_universal_row(
    row: dict[str, Any],
    mapping: dict[str, Any],
    *,
    confidence: float = 0.82,
    schema_source: str = "inferred",
) -> dict[str, Any]:
    """Map columns to semantic roles and merge with preserved fields."""
    _ = confidence  # confidence finalized in universal pipeline
    src = schema_source if schema_source != "inferred" else "heuristic"
    return mapped_intelligence_row(row, mapping, schema_source=src)


def mapping_is_non_empty(mapping: dict[str, Any]) -> bool:
    return bool(mapping) and any(v for v in mapping.values() if v)
