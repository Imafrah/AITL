"""
Universal value cleaning utilities — names, dates (ISO), amounts, contact fields.

All functions operate on individual VALUES — no column-name assumptions.
Schema-agnostic: these are type-appropriate transformers, not field-specific rules.
"""

from __future__ import annotations

import re
from typing import Any

from parsers.csv_parser import safe_float
from post_processor.processor import parse_date

_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
_PHONE_DIGITS_RE = re.compile(r"\D+")

# ── Common city name aliases (normalization of alternative spellings) ─────────
# These normalize alternative names for the SAME entity — not schema assumptions.

_CITY_ALIASES: dict[str, str] = {
    "bengaluru": "Bangalore",
    "bangalore": "Bangalore",
    "gurgaon": "Gurugram",
    "gurugram": "Gurugram",
    "bombay": "Mumbai",
    "calcutta": "Kolkata",
}


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
