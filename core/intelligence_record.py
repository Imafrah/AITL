"""
Intelligence rows: semantic slots (context-aware), sparse output, preserved unmapped columns.
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
    normalize_city,
    normalize_date_value,
    normalize_status_value,
)
from core.semantic_mapping import classify_fields, dynamic_semantic_map
from parsers.csv_parser import normalize_field_name

_LEGACY_KEYS = frozenset({"person_name", "organization", "amount", "is_outlier"})


def _smart_clean_cell(norm_key: str, value: Any) -> Any:
    if value is None:
        return None
    nk = norm_key
    if "email" in nk or nk in ("e_mail", "mail"):
        return clean_email(str(value))
    if any(x in nk for x in ("phone", "tel", "mobile", "cell", "fax")):
        return clean_phone(str(value))
    if any(x in nk for x in ("city", "town", "municipality")):
        return normalize_city(str(value))
    if "name" in nk and "company" not in nk and "org" not in nk:
        return clean_name(str(value))
    if "date" in nk or nk in ("dob", "birth", "timestamp", "created", "updated"):
        iso = normalize_date_value(value)
        if iso:
            return iso
        s = str(value).strip()
        return s[:128] if s else None
    if any(
        x in nk
        for x in (
            "quantity",
            "qty",
            "units",
            "items",
        )
    ):
        n = amount_from_value(value)
        return n
    if any(
        x in nk
        for x in (
            "salary",
            "amount",
            "price",
            "total",
            "cost",
            "wage",
            "pay",
            "revenue",
            "balance",
        )
    ):
        return amount_from_value(value)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value) if value == value else None
    if isinstance(value, str):
        s = value.strip()
        return s[:2048] if s else None
    return value


def preserve_csv_row(row: dict[str, Any]) -> dict[str, Any]:
    """All columns → normalized snake_case keys with light type-aware cleaning."""
    out: dict[str, Any] = {}
    for k, v in row.items():
        nk = normalize_field_name(str(k))
        if not nk:
            continue
        out[nk] = _smart_clean_cell(nk, v)
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


def _resolve_output_amount(sem: dict[str, Any]) -> tuple[float | None, Any]:
    """Prefer general monetary columns (sorted) then compensation-style columns — never quantity."""
    raw_amt = sem.get("amount_monetary")
    raw_sal = sem.get("salary_comp")
    if raw_amt is not None:
        v = amount_from_value(raw_amt)
        if v is not None:
            return v, raw_amt
    if raw_sal is not None:
        v = amount_from_value(raw_sal)
        if v is not None:
            return v, raw_sal
    return None, None


def semantic_intelligence_row(
    row: dict[str, Any],
    field_map: dict[str, list[str]],
    *,
    schema_source: str = "heuristic",
) -> dict[str, Any]:
    """Map row through field_map; sparse canonical fields + preserved extras + validation flags."""
    sem = dynamic_semantic_map(row, field_map)
    preserved = preserve_csv_row(row)

    used_norms = {normalize_field_name(str(c)) for cols in field_map.values() for c in cols}

    person_name = clean_name(sem.get("person_name"))
    email = clean_email(sem.get("email"))
    phone = clean_phone(sem.get("phone"))
    location = normalize_city(sem.get("location"))
    status = normalize_status_value(sem.get("status"))

    qty_raw = sem.get("quantity")
    quantity = _quantity_parsed(qty_raw)

    amount, amt_raw = _resolve_output_amount(sem)

    date_raw = sem.get("date")
    date_val = normalize_date_value(date_raw)

    rec: dict[str, Any] = {}

    if person_name:
        rec["person_name"] = person_name
    if email:
        rec["email"] = email
    if phone:
        rec["phone"] = phone
    if quantity is not None:
        rec["quantity"] = quantity
    if amount is not None:
        rec["amount"] = amount
    if date_val:
        rec["date"] = date_val
    if status:
        rec["status"] = status
    if location:
        rec["city"] = location

    if person_name:
        rec["name"] = person_name

    # Validation flags
    rec["is_valid_email"] = bool(email and is_valid_email(email))
    if date_raw is not None and str(date_raw).strip():
        rec["is_valid_date"] = is_valid_date(date_raw)
    else:
        rec["is_valid_date"] = True
    has_qty_role = bool(field_map.get("quantity"))
    has_amt_role = bool(field_map.get("amount_monetary") or field_map.get("salary_comp"))
    qty_ok = True
    if has_qty_role and qty_raw is not None and str(qty_raw).strip():
        qty_ok = is_valid_numeric(qty_raw)
    amt_ok = True
    if has_amt_role and amt_raw is not None and str(amt_raw).strip():
        amt_ok = is_valid_numeric(amt_raw)
    rec["is_valid_numeric"] = qty_ok and amt_ok

    _ = schema_source  # reserved for future provenance boosts; confidence finalized in pipeline
    rec["confidence"] = 1.0
    rec["is_anomaly"] = False

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
    """Backward-compatible entry when caller has a merged field_map (same as semantic)."""
    fm = {k: (v if isinstance(v, list) else [v]) for k, v in mapping.items() if v}
    return semantic_intelligence_row(row, fm, schema_source=schema_source)


def heuristic_intelligence_row(row: dict[str, Any]) -> dict[str, Any]:
    keys = [k for k in row.keys() if k is not None]
    fm = classify_fields([str(k) for k in keys])
    return semantic_intelligence_row(row, fm, schema_source="heuristic")


def coerce_intelligence_row(d: dict[str, Any]) -> dict[str, Any]:
    """Normalize legacy / unstructured rows into the semantic shape."""
    out: dict[str, Any] = {}
    for k, v in d.items():
        if k in _LEGACY_KEYS:
            continue
        if k in (
            "is_valid_email",
            "is_valid_date",
            "is_valid_numeric",
            "confidence",
            "is_anomaly",
        ):
            continue
        out[k] = v

    pn = clean_name(d.get("person_name") or d.get("name"))
    if pn:
        out["person_name"] = pn
        out["name"] = pn
    elif d.get("person_name") is None and d.get("name") is None:
        out.pop("person_name", None)
        out.pop("name", None)

    em = clean_email(d.get("email"))
    if em:
        out["email"] = em

    ph = clean_phone(d.get("phone"))
    if ph:
        out["phone"] = ph

    loc = normalize_city(d.get("location") or d.get("city") or d.get("organization"))
    if loc:
        out["location"] = loc
        out["city"] = loc

    amt = amount_from_value(d.get("amount"))
    if amt is None:
        amt = amount_from_value(d.get("salary"))
    if amt is not None:
        out["amount"] = amt

    q = _quantity_parsed(d.get("quantity"))
    if q is not None:
        out["quantity"] = q

    st = normalize_status_value(d.get("status"))
    if st:
        out["status"] = st

    iso = normalize_date_value(d.get("date"))
    if iso:
        out["date"] = iso
    elif d.get("date") is not None and str(d.get("date")).strip():
        out["date"] = str(d.get("date")).strip()[:32]

    out["is_valid_email"] = bool(out.get("email") and is_valid_email(out.get("email")))
    if out.get("date"):
        out["is_valid_date"] = is_valid_date(out.get("date"))
    else:
        out["is_valid_date"] = True
    amt_ok = True
    if d.get("amount") is not None or d.get("salary") is not None:
        amt_ok = is_valid_numeric(
            d.get("amount") if d.get("amount") is not None else d.get("salary")
        )
    qty_ok = True
    if d.get("quantity") is not None:
        qty_ok = is_valid_numeric(d.get("quantity"))
    out["is_valid_numeric"] = amt_ok and qty_ok

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
    """Drop duplicates by (person_name|name, email, phone)."""
    seen: set[tuple[Any, ...]] = set()
    out: list[dict[str, Any]] = []
    for r in rows:
        n = (r.get("person_name") or r.get("name") or "").strip().lower()
        e = (r.get("email") or "").strip().lower()
        p = phone_fingerprint(r.get("phone"))
        key = (n, e, p)
        if key == ("", "", ""):
            key = ("__empty__", hash(frozenset((k, str(v)) for k, v in sorted(r.items()))))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out
