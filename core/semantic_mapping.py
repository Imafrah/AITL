"""
Context-aware semantic column classification: keyword similarity (no exact-name tables),
dataset-type hints, and row-level semantic extraction. Extensible via keyword sets only.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from parsers.csv_parser import normalize_field_name

logger = logging.getLogger(__name__)

# --- Dataset-type signals (substring / token overlap on normalized headers) -----------------
_DATASET_KEYWORDS: dict[str, frozenset[str]] = {
    "invoice": frozenset(
        {
            "invoice",
            "billing",
            "product",
            "sku",
            "line",
            "quantity",
            "qty",
            "purchase",
            "tax_id",
            "po_",
            "vendor_bill",
            "receipt",
        }
    ),
    "employee": frozenset(
        {
            "salary",
            "department",
            "employee",
            "hire",
            "payroll",
            "staff",
            "position",
            "job_title",
            "dob",
            "benefits",
            "compensation",
            "wage",
        }
    ),
    "transaction": frozenset(
        {
            "transaction",
            "payment",
            "merchant",
            "card",
            "txn",
            "transfer",
            "reference",
            "authorization",
        }
    ),
    "sales": frozenset(
        {
            "sales",
            "revenue",
            "order",
            "customer_id",
            "ship",
            "fulfill",
        }
    ),
}

# Semantic roles (priority order for assignment: first match wins per column).
# quantity is strictly separated from monetary amount / salary.
_ROLE_SPECS: tuple[tuple[str, frozenset[str]], ...] = (
    ("email", frozenset({"email", "e_mail", "mail", "mailbox"})),
    ("phone", frozenset({"phone", "tel", "mobile", "cell", "fax", "whatsapp"})),
    (
        "quantity",
        frozenset(
            {
                "quantity",
                "qty",
                "units",
                "items_ordered",
                "units_sold",
                "pieces",
                "count_items",
                "num_items",
            }
        ),
    ),
    ("status", frozenset({"status", "phase", "payment_status", "order_status", "workflow"})),
    (
        "date",
        frozenset(
            {
                "date",
                "time",
                "timestamp",
                "due",
                "invoice_date",
                "created",
                "updated",
                "dob",
                "birth",
            }
        ),
    ),
    (
        "organization",
        frozenset(
            {
                "company",
                "vendor",
                "supplier",
                "department",
                "merchant",
                "employer",
                "org",
                "company_name",
            }
        ),
    ),
    (
        "person_name",
        frozenset(
            {
                "name",
                "person",
                "customer",
                "client",
                "buyer",
                "employee",
                "member",
                "contact",
                "full_name",
                "first_name",
                "last_name",
            }
        ),
    ),
    (
        "location",
        frozenset(
            {
                "city",
                "town",
                "address",
                "region",
                "zip",
                "postal",
                "state",
                "country",
                "location",
            }
        ),
    ),
    (
        "salary_comp",
        frozenset(
            {
                "salary",
                "wage",
                "payroll",
                "compensation",
                "base_salary",
                "annual_salary",
                "hourly",
                "bonus",
                "stipend",
            }
        ),
    ),
    (
        "amount_monetary",
        frozenset(
            {
                "amount",
                "price",
                "total",
                "cost",
                "subtotal",
                "tax",
                "fee",
                "balance",
                "payment",
                "net",
                "gross",
                "grand_total",
                "line_total",
                "revenue",
                "paid",
                "due",
                "value",
            }
        ),
    ),
    ("currency", frozenset({"currency", "curr", "iso", "fx"})),
)


def _header_matches_keywords(norm_header: str, keywords: frozenset[str]) -> bool:
    if not norm_header:
        return False
    for kw in keywords:
        if len(kw) <= 2 and norm_header != kw:
            continue
        if norm_header == kw:
            return True
        if len(kw) >= 3 and kw in norm_header:
            return True
        if norm_header.startswith(kw + "_") or norm_header.endswith("_" + kw):
            return True
    return False


def detect_dataset_type(columns: list[str]) -> str:
    """
    Score dataset kinds from normalized header tokens (keyword overlap).
    Returns one of: invoice, employee, transaction, sales, generic.
    """
    norms = [normalize_field_name(str(c)) for c in columns if c is not None and str(c).strip()]
    if not norms:
        return "generic"

    best = "generic"
    best_score = 0
    for dtype, kws in _DATASET_KEYWORDS.items():
        score = 0
        for col in norms:
            for kw in kws:
                if len(kw) >= 3 and kw in col:
                    score += 2
                elif col == kw:
                    score += 3
        if score > best_score:
            best_score = score
            best = dtype

    if best_score == 0:
        return "generic"
    logger.info("Detected dataset type: %s (score=%s)", best, best_score)
    return best


def classify_fields(columns: list[str]) -> dict[str, list[str]]:
    """
    Map semantic roles → list of original column names (each column assigned once).
    quantity and salary_comp are never merged with amount_monetary.
    """
    field_map: dict[str, list[str]] = {}
    used: set[str] = set()

    for col in columns:
        if col is None or not str(col).strip():
            continue
        orig = str(col).strip()
        if orig in used:
            continue
        nk = normalize_field_name(orig)
        if not nk:
            continue
        assigned = False
        for role, kws in _ROLE_SPECS:
            if _header_matches_keywords(nk, kws):
                field_map.setdefault(role, []).append(orig)
                used.add(orig)
                assigned = True
                break
        if not assigned:
            logger.debug("Skipped invalid field mapping | column=%r (no semantic role)", orig)

    nonempty = {k: v for k, v in field_map.items() if v}
    logger.info("Using semantic mapping for fields: %s", list(nonempty.keys()))

    unmapped = [str(c).strip() for c in columns if c and str(c).strip() and str(c).strip() not in used]
    if unmapped:
        logger.info(
            "Skipped invalid field mapping | unmapped_columns=%s",
            unmapped[:20],
        )

    return field_map


def _sort_amount_columns(cols: list[str]) -> list[str]:
    """Prefer totals / line amounts over unit price when several money columns exist."""

    def sort_key(c: str) -> tuple[int, str]:
        n = normalize_field_name(str(c))
        if "grand" in n or n == "total" or n.endswith("_total") or "total_amount" in n:
            return (0, c)
        if "line" in n and "total" in n:
            return (1, c)
        if "subtotal" in n:
            return (2, c)
        if "net" in n or "gross" in n:
            return (3, c)
        if "tax" in n or "fee" in n or "discount" in n:
            return (4, c)
        if "balance" in n or "due" in n or "paid" in n:
            return (5, c)
        if "unit" in n and "price" in n:
            return (8, c)
        if "price" in n or "rate" in n or "cost" in n:
            return (7, c)
        return (6, c)

    return sorted(cols, key=sort_key)


def dynamic_semantic_map(row: dict[str, Any], field_map: dict[str, Any]) -> dict[str, Any]:
    """First non-empty cell per semantic role (original CSV keys)."""
    out: dict[str, Any] = {}
    for role, cols in field_map.items():
        if cols is None:
            continue
        col_list = cols if isinstance(cols, list) else [cols]
        col_list = list(col_list)
        if role == "amount_monetary":
            col_list = _sort_amount_columns(col_list)
        for c in col_list:
            if c not in row:
                continue
            v = row.get(c)
            if v is None:
                continue
            if isinstance(v, str) and not v.strip():
                continue
            out[role] = v
            break
    return out


def _ai_role_aliases() -> dict[str, str]:
    """Map AI / legacy schema keys onto internal roles."""
    return {
        "amount": "amount_monetary",
        "person_name": "person_name",
        "organization": "organization",
        "date": "date",
        "currency": "currency",
        "email": "email",
        "phone": "phone",
        "city": "location",
        "quantity": "quantity",
    }


def merge_field_maps(
    base: dict[str, list[str]],
    overlay: dict[str, Any] | None,
) -> dict[str, list[str]]:
    """Union column lists per role; AI keys are aliased to internal roles."""
    out: dict[str, list[str]] = {k: list(v) for k, v in base.items()}
    if not overlay:
        return out
    aliases = _ai_role_aliases()
    for key, cols in overlay.items():
        role = aliases.get(str(key), str(key))
        if role not in out:
            out[role] = []
        seq = cols if isinstance(cols, list) else [cols]
        for c in seq:
            if c is None:
                continue
            s = str(c).strip()
            if s and s not in out[role]:
                out[role].append(s)
    return out


def field_map_nonempty(field_map: dict[str, Any]) -> bool:
    return bool(field_map) and any(v for v in field_map.values() if v)


def field_map_needs_ai(field_map: dict[str, Any]) -> bool:
    if not field_map_nonempty(field_map):
        return True
    filled = sum(1 for v in field_map.values() if v)
    return filled < 2
