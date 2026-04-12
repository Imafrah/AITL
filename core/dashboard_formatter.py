"""Compact dashboard payload: summary, chart-ready aggregates, sample rows."""

from __future__ import annotations

from typing import Any

from core.analytics_engine import compute_analytics


def build_dashboard(
    records: list[dict[str, Any]],
    analytics: dict[str, Any] | None = None,
    *,
    preview_limit: int = 10,
) -> dict[str, Any]:
    a = analytics if analytics is not None else compute_analytics(records)
    salaries = [
        float(r["salary"])
        for r in records
        if r.get("salary") is not None
        and isinstance(r["salary"], (int, float))
        and not isinstance(r["salary"], bool)
        and r["salary"] == r["salary"]
    ]

    return {
        "summary": {
            "total_records": a.get("total_records", 0),
            "avg_salary": a.get("avg_salary"),
            "max_salary": a.get("max_salary"),
            "min_salary": a.get("min_salary"),
            "records_with_salary": a.get("records_with_salary", 0),
            "unique_cities": len(a.get("city_distribution") or {}),
        },
        "charts": {
            "salary_stats": {
                "min": a.get("min_salary"),
                "max": a.get("max_salary"),
                "avg": a.get("avg_salary"),
                "count": len(salaries),
            },
            "city_distribution": a.get("city_distribution") or {},
        },
        "records": records[:preview_limit],
    }
