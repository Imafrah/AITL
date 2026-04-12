"""Dataset-level analytics for dashboard and metadata (dynamic, no fixed schema)."""

from __future__ import annotations

from collections import Counter
from typing import Any


def _numeric_salaries(records: list[dict[str, Any]]) -> list[float]:
    out: list[float] = []
    for r in records:
        v = r.get("salary")
        if isinstance(v, (int, float)) and not isinstance(v, bool) and v == v:
            out.append(float(v))
    return out


def compute_analytics(records: list[dict[str, Any]]) -> dict[str, Any]:
    salaries = _numeric_salaries(records)
    cities = Counter()
    for r in records:
        c = r.get("city")
        if c is None:
            continue
        s = str(c).strip()
        if s:
            cities[s] += 1

    avg = sum(salaries) / len(salaries) if salaries else None
    return {
        "total_records": len(records),
        "avg_salary": round(avg, 4) if avg is not None else None,
        "max_salary": max(salaries) if salaries else None,
        "min_salary": min(salaries) if salaries else None,
        "records_with_salary": len(salaries),
        "city_distribution": dict(cities.most_common(100)),
    }
