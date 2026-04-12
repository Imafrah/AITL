"""Fully dynamic analytics: any numeric column → min / max / avg / count."""

from __future__ import annotations

from collections import Counter
from typing import Any


def _mean(vals: list[float]) -> float | None:
    return round(sum(vals) / len(vals), 4) if vals else None


def _is_numeric_value(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and v == v


_RESERVED = frozenset(
    {
        "confidence",
        "is_anomaly",
        "is_valid_email",
        "is_valid_date",
        "is_valid_numeric",
    }
)


def compute_analytics(records: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Scan all records for numeric fields (no assumed salary/amount column names).
    Also builds city distribution from ``city`` / ``location`` when present.
    """
    numeric_buckets: dict[str, list[float]] = {}
    cities = Counter()

    for r in records:
        for k, v in r.items():
            if k in _RESERVED or k.startswith("_"):
                continue
            if _is_numeric_value(v):
                numeric_buckets.setdefault(k, []).append(float(v))
        for loc_key in ("city", "location"):
            c = r.get(loc_key)
            if c is None:
                continue
            s = str(c).strip()
            if s:
                cities[s] += 1
                break

    numeric_columns: dict[str, Any] = {}
    for k, vals in numeric_buckets.items():
        numeric_columns[k] = {
            "avg": _mean(vals),
            "min": min(vals),
            "max": max(vals),
            "count": len(vals),
        }

    return {
        "total_records": len(records),
        "numeric_columns": numeric_columns,
        "city_distribution": dict(cities.most_common(100)),
        # Backward-compatible flat keys for dashboards that still read these:
        "avg_salary": None,
        "max_salary": None,
        "min_salary": None,
        "records_with_salary": 0,
        "numeric_summary": {
            "amount": numeric_columns.get("amount", {"avg": None, "min": None, "max": None, "count": 0}),
            "quantity": numeric_columns.get(
                "quantity", {"avg": None, "min": None, "max": None, "count": 0}
            ),
        },
    }
