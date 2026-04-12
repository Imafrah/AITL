"""Compact dashboard payload driven by dataset-aware analytics."""

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
    dt = a.get("dataset_type") or "generic"
    ns = a.get("numeric_summary") or {}
    amt = ns.get("amount") or {}
    qty = ns.get("quantity") or {}

    summary = {
        "total_records": a.get("total_records", 0),
        "dataset_type": dt,
        "primary_metric": a.get("primary_metric"),
        "unique_cities": len(a.get("city_distribution") or {}),
    }
    if dt == "employee":
        summary["avg_salary"] = a.get("avg_salary")
        summary["max_salary"] = a.get("max_salary")
        summary["min_salary"] = a.get("min_salary")
    else:
        summary["avg_amount"] = a.get("avg_amount") or amt.get("avg")
        summary["avg_quantity"] = a.get("avg_quantity") or qty.get("avg")

    charts = {
        "amount_stats": {
            "min": amt.get("min"),
            "max": amt.get("max"),
            "avg": amt.get("avg"),
            "count": amt.get("count", 0),
        },
        "quantity_stats": {
            "min": qty.get("min"),
            "max": qty.get("max"),
            "avg": qty.get("avg"),
            "count": qty.get("count", 0),
        },
        "city_distribution": a.get("city_distribution") or {},
    }

    return {
        "summary": summary,
        "charts": charts,
        "records": records[:preview_limit],
    }
