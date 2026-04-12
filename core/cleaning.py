"""Universal row cleaning — names, dates (ISO), amounts, invalid stripping, outliers."""

from __future__ import annotations

import re
from typing import Any

from parsers.csv_parser import safe_float
from post_processor.processor import parse_date


def clean_name(value: str | None) -> str | None:
    if value is None:
        return None
    t = str(value).strip()
    if not t:
        return None
    t = re.sub(r"\s+", " ", t)
    return t[:512] or None


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
