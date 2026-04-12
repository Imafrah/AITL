"""Universal row cleaning — names, dates (ISO), amounts, contact fields, outliers."""

from __future__ import annotations

import re
from typing import Any

from parsers.csv_parser import safe_float
from post_processor.processor import parse_date

_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
_PHONE_DIGITS_RE = re.compile(r"\D+")


def clean_name(value: str | None) -> str | None:
    if value is None:
        return None
    t = str(value).strip()
    if not t:
        return None
    t = re.sub(r"\s+", " ", t)
    return t[:512] or None


def clean_email(value: str | None) -> str | None:
    if value is None:
        return None
    s = str(value).strip().lower()
    if not s:
        return None
    return s[:320] or None


def clean_phone(value: str | None) -> str | None:
    if value is None:
        return None
    s = re.sub(r"\s+", " ", str(value).strip())
    if not s:
        return None
    return s[:64] or None


_CITY_ALIASES: dict[str, str] = {
    "bengaluru": "Bangalore",
    "bangalore": "Bangalore",
    "gurgaon": "Gurugram",
    "gurugram": "Gurugram",
    "bombay": "Mumbai",
    "calcutta": "Kolkata",
}


def normalize_city(value: str | None) -> str | None:
    """Light normalization for city / location labels (title case + common aliases)."""
    base = clean_name(value)
    if not base:
        return None
    key = base.strip().lower()
    if key in _CITY_ALIASES:
        return _CITY_ALIASES[key]
    return base.title()


def normalize_status_value(value: str | None) -> str | None:
    """Human-readable status: e.g. paid → Paid, in_progress → In Progress."""
    s = clean_name(value)
    if not s:
        return None
    if re.search(r"[_\s-]", s):
        parts = re.split(r"[_\s-]+", s)
        return " ".join(p.capitalize() for p in parts if p)[:256] or None
    return s.capitalize()[:256]


def is_valid_email(value: str | None) -> bool:
    s = clean_email(value)
    return bool(s and _EMAIL_RE.match(s))


def is_valid_phone(value: str | None) -> bool:
    s = clean_phone(value)
    if not s:
        return False
    digits = _PHONE_DIGITS_RE.sub("", s)
    return 10 <= len(digits) <= 15


def normalize_date_value(value: Any) -> str | None:
    """Return YYYY-MM-DD when parseable, else None."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    return parse_date(s)


def amount_from_value(value: Any) -> float | None:
    """Numeric amount; None if invalid or empty."""
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if value != value:  # NaN
            return None
        return float(value)
    return safe_float(str(value).strip())


def is_valid_date(value: Any) -> bool:
    return normalize_date_value(value) is not None


def is_valid_numeric(value: Any) -> bool:
    """True if value parses to a finite float (empty / None → False)."""
    if value is None:
        return False
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value == value
    a = amount_from_value(value)
    return a is not None and a == a


def is_valid_salary(value: Any) -> bool:
    """True when numeric and in a plausible compensation range."""
    a = amount_from_value(value)
    if a is None:
        return False
    return 0.0 <= a <= 100_000_000.0


def build_clean_row(
    person_name: str | None,
    organization: str | None,
    amount: Any,
    date_val: Any,
    confidence: float,
) -> dict[str, Any]:
    """Apply universal cleaning to one logical row."""
    amt = amount_from_value(amount)
    iso = normalize_date_value(date_val)
    return {
        "person_name": clean_name(person_name),
        "organization": clean_name(organization),
        "amount": amt,
        "date": iso,
        "confidence": max(0.0, min(1.0, float(confidence))),
        "is_outlier": False,
    }


def mark_amount_outliers(rows: list[dict[str, Any]]) -> None:
    """Flag statistical outliers on amount (IQR); no-op if too few numeric amounts."""
    nums: list[float] = []
    for r in rows:
        a = r.get("amount")
        if isinstance(a, (int, float)) and not isinstance(a, bool) and a == a:
            nums.append(float(a))
    if len(nums) < 4:
        return
    nums.sort()
    n = len(nums)
    q1 = nums[n // 4]
    q3 = nums[(3 * n) // 4]
    iqr = q3 - q1
    if iqr <= 0:
        iqr = abs(q3) or 1.0
    low = q1 - 1.5 * iqr
    high = q3 + 1.5 * iqr
    for r in rows:
        a = r.get("amount")
        if isinstance(a, (int, float)) and not isinstance(a, bool):
            if float(a) < low or float(a) > high:
                r["is_outlier"] = True
