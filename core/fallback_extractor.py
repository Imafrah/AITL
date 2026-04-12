"""
Lightweight regex-based extraction when the AI service is unavailable or fails.
Does not replace the full pipeline — only guarantees non-empty structured rows.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from post_processor.processor import parse_date

logger = logging.getLogger(__name__)

# ISO-like and common numeric dates
_DATE_PATTERNS = [
    re.compile(r"\b(\d{4}-\d{2}-\d{2})\b"),
    re.compile(r"\b(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})\b"),
]

# Money / large numbers (avoid tiny integers as noise)
_AMOUNT_PATTERN = re.compile(
    r"(?:[$€£₹]\s*)?(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+\.\d{2}\b|\b\d{4,}\b)"
)

# Two or more capitalized words (simple "person-like" heuristic)
_NAME_PATTERN = re.compile(
    r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b"
)

_MAX_ROWS = 10


def _find_dates(text: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for pat in _DATE_PATTERNS:
        for m in pat.finditer(text):
            raw = m.group(1)
            iso = parse_date(raw)
            key = iso or raw
            if key and key not in seen:
                seen.add(key)
                out.append(iso or raw.strip()[:32])
    return out


def _find_amounts(text: str) -> list[float]:
    out: list[float] = []
    seen: set[float] = set()
    for m in _AMOUNT_PATTERN.finditer(text):
        raw = m.group(1).replace(",", "")
        try:
            v = float(raw)
        except ValueError:
            continue
        # Avoid standalone years masquerading as amounts
        if v == int(v) and 1900 <= v <= 2100 and len(raw) <= 4:
            continue
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
        if len(out) >= _MAX_ROWS:
            break
    return out


def _find_names(text: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for m in _NAME_PATTERN.finditer(text):
        name = m.group(1).strip()
        low = name.lower()
        if low in seen or len(name) < 4:
            continue
        # Skip common false positives
        if low in ("total amount", "invoice date", "due date", "payment method"):
            continue
        seen.add(low)
        out.append(name)
        if len(out) >= _MAX_ROWS:
            break
    return out


def _empty_row(confidence: float = 0.35) -> dict[str, Any]:
    return {
        "person_name": None,
        "organization": None,
        "amount": None,
        "date": None,
        "confidence": confidence,
        "is_outlier": False,
        "name": None,
        "email": None,
        "phone": None,
        "city": None,
        "salary": None,
        "is_anomaly": False,
    }


def fallback_extract(text: str) -> list[dict[str, Any]]:
    """
    Regex-only extraction: names, amounts, dates.
    Always returns at least one row (possibly all-null) so callers never ship [].
    """
    if not text or not str(text).strip():
        return [_empty_row()]

    sample = str(text)[:200_000]
    dates = _find_dates(sample)
    amounts = _find_amounts(sample)
    names = _find_names(sample)

    if not names and not amounts and not dates:
        logger.info("Fallback: no regex matches; using placeholder row")
        return [_empty_row(0.4)]

    n = min(_MAX_ROWS, max(len(names), len(amounts), len(dates), 1))
    rows: list[dict[str, Any]] = []
    for i in range(n):
        pn = names[i] if i < len(names) else (names[0] if names else None)
        amt = amounts[i] if i < len(amounts) else (amounts[0] if amounts else None)
        dt = dates[i] if i < len(dates) else (dates[0] if dates else None)
        iso = parse_date(str(dt)) if dt else None
        rows.append(
            {
                "person_name": pn,
                "name": pn,
                "organization": None,
                "city": None,
                "email": None,
                "phone": None,
                "amount": amt,
                "salary": amt,
                "date": iso or (str(dt)[:32] if dt else None),
                "confidence": 0.5,
                "is_outlier": False,
                "is_anomaly": False,
            }
        )

    return rows
