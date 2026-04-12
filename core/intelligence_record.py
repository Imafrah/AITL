"""
Row model for the universal intelligence engine: preserve all CSV fields (normalized keys),
canonical slots (name, email, phone, city, salary), confidence, and anomaly flag.
"""

from __future__ import annotations

import re
from typing import Any

from core.cleaning import (
    amount_from_value,
    clean_email,
    clean_name,
    clean_phone,
    is_valid_email,
    is_valid_phone,
    normalize_city,
    normalize_date_value,
)
from parsers.csv_parser import dynamic_map_row, normalize_field_name, safe_float
from post_processor.processor import parse_date

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
    """All columns → normalized snake_case keys with light type-aware cleaning (no drops)."""
    out: dict[str, Any] = {}
    for k, v in row.items():
        nk = normalize_field_name(str(k))
        if not nk:
            continue
        out[nk] = _smart_clean_cell(nk, v)
    return out


def _confidence_for_record(
    rec: dict[str, Any],
    *,
    schema_source: str,
) -> float:
    base = {"memory": 0.96, "ai": 0.9, "heuristic": 0.82, "migrated": 0.84}.get(
        schema_source, 0.85
    )
    if rec.get("name") and not is_valid_email(rec.get("email")) and rec.get("email"):
        base -= 0.06
    if rec.get("email") and is_valid_email(rec.get("email")):
        base = min(1.0, base + 0.02)
    if rec.get("phone") and not is_valid_phone(rec.get("phone")):
        base -= 0.04
    if rec.get("phone") and is_valid_phone(rec.get("phone")):
        base = min(1.0, base + 0.01)
    if not (rec.get("name") or rec.get("email") or rec.get("phone")):
        base -= 0.12
    return max(0.0, min(1.0, base))


def mapped_intelligence_row(
    row: dict[str, Any],
    mapping: dict[str, Any],
    *,
    schema_source: str = "heuristic",
) -> dict[str, Any]:
    preserved = preserve_csv_row(row)
    m = dynamic_map_row(row, mapping)

    name = clean_name(m.get("person_name")) or clean_name(preserved.get("name"))
    email = clean_email(m.get("email")) or clean_email(preserved.get("email"))
    phone = clean_phone(m.get("phone")) or clean_phone(preserved.get("phone"))
    city = (
        normalize_city(m.get("city"))
        or normalize_city(m.get("organization"))
        or normalize_city(preserved.get("city"))
        or normalize_city(preserved.get("organization"))
    )
    salary = amount_from_value(m.get("amount"))
    if salary is None:
        salary = amount_from_value(preserved.get("salary"))
    if salary is None:
        salary = amount_from_value(preserved.get("amount"))

    out = {**preserved}
    out["name"] = name
    out["email"] = email
    out["phone"] = phone
    out["city"] = city
    out["salary"] = salary
    dv = m.get("date")
    if dv not in (None, ""):
        iso = normalize_date_value(dv)
        if iso:
            out["date"] = iso
        elif "date" not in out or out.get("date") is None:
            out["date"] = str(dv).strip()[:32] or None
    out["confidence"] = _confidence_for_record(out, schema_source=schema_source)
    out["is_anomaly"] = False
    return out


def heuristic_intelligence_row(row: dict[str, Any]) -> dict[str, Any]:
    preserved = preserve_csv_row(row)
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

    name = clean_name(preserved.get("name")) or clean_name(texts[0] if texts else None)
    email = clean_email(preserved.get("email"))
    phone = clean_phone(preserved.get("phone"))
    city = normalize_city(preserved.get("city"))
    salary = amount_from_value(preserved.get("salary")) or (
        nums[0] if nums else amount_from_value(preserved.get("amount"))
    )

    out = {**preserved}
    out["name"] = name
    out["email"] = email
    out["phone"] = phone
    out["city"] = city
    out["salary"] = salary
    if dates and not out.get("date"):
        out["date"] = dates[0]
    out["confidence"] = _confidence_for_record(out, schema_source="heuristic")
    out["is_anomaly"] = False
    return out


def coerce_intelligence_row(d: dict[str, Any]) -> dict[str, Any]:
    """Normalize legacy or partial rows into the intelligence shape; keep extra keys."""
    out: dict[str, Any] = {}
    for k, v in d.items():
        if k in _LEGACY_KEYS:
            continue
        out[k] = v

    out["name"] = clean_name(d.get("name") or d.get("person_name"))
    out["email"] = clean_email(d.get("email"))
    out["phone"] = clean_phone(d.get("phone"))
    out["city"] = normalize_city(d.get("city") or d.get("organization"))
    out["salary"] = amount_from_value(
        d.get("salary") if d.get("salary") is not None else d.get("amount")
    )
    iso = normalize_date_value(d.get("date"))
    if iso:
        out["date"] = iso
    elif d.get("date") is not None and str(d.get("date")).strip():
        out["date"] = str(d.get("date")).strip()[:32]
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
    """Drop duplicates by (name, email, phone); empty triples use row content hash."""
    seen: set[tuple[Any, ...]] = set()
    out: list[dict[str, Any]] = []
    for r in rows:
        n = (r.get("name") or "").strip().lower()
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
