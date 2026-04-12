"""Dataset-aware analytics: employee vs invoice vs generic (no salary stats on invoice-only data)."""

from __future__ import annotations

from collections import Counter
from typing import Any


def _mean(vals: list[float]) -> float | None:
    return round(sum(vals) / len(vals), 4) if vals else None


def compute_analytics(
    records: list[dict[str, Any]],
    dataset_type: str | None = None,
) -> dict[str, Any]:
    """
    Build summaries from semantic fields. ``dataset_type`` drives labels and which
    metrics are emphasized (e.g. compensation only for employee-like data).
    """
    dt = (dataset_type or "generic").lower()
    if dt not in ("invoice", "employee", "transaction", "sales", "generic"):
        dt = "generic"

    amounts: list[float] = []
    quantities: list[float] = []
    cities = Counter()

    for r in records:
        a = r.get("amount")
        if isinstance(a, (int, float)) and not isinstance(a, bool) and a == a:
            amounts.append(float(a))
        q = r.get("quantity")
        if isinstance(q, (int, float)) and not isinstance(q, bool) and q == q:
            quantities.append(float(q))
        for loc_key in ("location", "city"):
            c = r.get(loc_key)
            if c is None:
                continue
            s = str(c).strip()
            if s:
                cities[s] += 1
                break

    numeric_summary: dict[str, Any] = {
        "amount": {
            "avg": _mean(amounts),
            "min": min(amounts) if amounts else None,
            "max": max(amounts) if amounts else None,
            "count": len(amounts),
        },
        "quantity": {
            "avg": _mean(quantities),
            "min": min(quantities) if quantities else None,
            "max": max(quantities) if quantities else None,
            "count": len(quantities),
        },
    }

    out: dict[str, Any] = {
        "total_records": len(records),
        "dataset_type": dt,
        "numeric_summary": numeric_summary,
        "city_distribution": dict(cities.most_common(100)),
    }

    # Backward-compatible keys + context-specific emphasis
    if dt == "employee":
        out["avg_salary"] = numeric_summary["amount"]["avg"]
        out["max_salary"] = numeric_summary["amount"]["max"]
        out["min_salary"] = numeric_summary["amount"]["min"]
        out["records_with_salary"] = numeric_summary["amount"]["count"]
        out["primary_metric"] = "compensation"
    elif dt in ("invoice", "sales", "transaction"):
        out["avg_salary"] = None
        out["max_salary"] = None
        out["min_salary"] = None
        out["records_with_salary"] = 0
        out["avg_amount"] = numeric_summary["amount"]["avg"]
        out["max_amount"] = numeric_summary["amount"]["max"]
        out["min_amount"] = numeric_summary["amount"]["min"]
        out["records_with_amount"] = numeric_summary["amount"]["count"]
        out["avg_quantity"] = numeric_summary["quantity"]["avg"]
        out["records_with_quantity"] = numeric_summary["quantity"]["count"]
        out["primary_metric"] = "invoice" if dt == "invoice" else "transaction"
    else:
        out["avg_salary"] = numeric_summary["amount"]["avg"] if amounts else None
        out["max_salary"] = max(amounts) if amounts else None
        out["min_salary"] = min(amounts) if amounts else None
        out["records_with_salary"] = len(amounts)
        out["primary_metric"] = "mixed"

    return out
