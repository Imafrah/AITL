"""Dashboard from dynamic analytics (numeric_columns)."""

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
    nc = a.get("numeric_columns") or {}

    # Surface top numeric columns by population for charts
    top_cols = sorted(nc.items(), key=lambda x: x[1].get("count", 0), reverse=True)[:5]

    charts: dict[str, Any] = {
        "numeric_series": {k: v for k, v in top_cols},
        "city_distribution": a.get("city_distribution") or {},
    }

    summary = {
        "total_records": a.get("total_records", 0),
        "numeric_field_count": len(nc),
        "top_numeric_fields": [k for k, _ in top_cols],
        "unique_cities": len(a.get("city_distribution") or {}),
    }

    return {
        "summary": summary,
        "charts": charts,
        "records": records[:preview_limit],
    }
