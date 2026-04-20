"""
Lightweight regex-based extraction when the AI service is unavailable or fails.
Does not replace the full pipeline — only guarantees non-empty structured rows.

Output is GENERIC — no hardcoded field names in row structure.
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

# Common false positives for name detection
_NAME_FALSE_POSITIVES = frozenset({
    "total amount", "invoice date", "due date", "payment method",
    "grand total", "sub total", "bank transfer", "credit card",
})


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
        if low in _NAME_FALSE_POSITIVES:
            continue
        seen.add(low)
        out.append(name)
        if len(out) >= _MAX_ROWS:
            break
    return out


def _empty_row(confidence: float = 0.35) -> dict[str, Any]:
    """Generic empty row — no hardcoded field names."""
    return {
        "confidence": confidence,
        "is_anomaly": False,
        "is_valid_email": True,
        "is_valid_date": True,
        "is_valid_numeric": True,
    }


def fallback_extract(text: str) -> list[dict[str, Any]]:
    """
    Regex-only extraction: names, amounts, dates.
    Always returns at least one row (possibly minimal) so callers never ship [].

    Output keys are generic labels based on detection patterns.
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
        row: dict[str, Any] = {}

        pn = names[i] if i < len(names) else (names[0] if names else None)
        amt = amounts[i] if i < len(amounts) else (amounts[0] if amounts else None)
        dt = dates[i] if i < len(dates) else (dates[0] if dates else None)
        iso = parse_date(str(dt)) if dt else None

        if pn:
            row["detected_name"] = pn
        if amt is not None:
            row["detected_amount"] = amt
        if iso or dt:
            row["detected_date"] = iso or (str(dt)[:32] if dt else None)

        row["confidence"] = 0.5
        row["is_anomaly"] = False
        row["is_valid_email"] = True
        row["is_valid_date"] = True if (not dt or iso) else False
        row["is_valid_numeric"] = True

        rows.append(row)

    return rows
