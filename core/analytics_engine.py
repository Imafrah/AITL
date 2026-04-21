"""Fully dynamic analytics: any numeric column → min / max / avg / count.

Analytics Safety Rule: only compute stats for columns where the meaning is clear.
Columns that look like ranks or indices (small integers 1–100, high uniqueness)
are SKIPPED to avoid surfacing misleading aggregate statistics.
"""

from __future__ import annotations

import logging
from collections import Counter
from typing import Any

logger = logging.getLogger(__name__)


def _mean(vals: list[float]) -> float | None:
    return round(sum(vals) / len(vals), 4) if vals else None


def _is_numeric_value(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and v == v


def _is_rank_or_index_values(vals: list[float]) -> bool:
    """
    Return True if the list of numeric values looks like ranks or indices.

    Criteria:
    - All values are integers
    - 80%+ fall within [1, 100]
    - There are at least 2 distinct values (not all the same)

    When True, analytics should be SKIPPED for this column.
    """
    if len(vals) < 2:
        return False
    integer_hits = sum(1 for v in vals if v == int(v))
    small_int_hits = sum(1 for v in vals if v == int(v) and 1 <= v <= 100)
    if integer_hits / len(vals) < 0.80:
        return False
    if small_int_hits / len(vals) < 0.80:
        return False
    distinct = len(set(int(v) for v in vals if v == int(v)))
    return distinct >= 2


_RESERVED = frozenset(
    {
        "confidence",
        "is_anomaly",
        "is_valid_email",
        "is_valid_date",
        "is_valid_numeric",
    }
)


def compute_analytics(
    records: list[dict[str, Any]],
    *,
    confirmed_numeric_cols: set[str] | None = None,
) -> dict[str, Any]:
    """
    Scan all records for numeric fields (no assumed salary/amount column names).
    Also builds city distribution from ``city`` / ``location`` when present.

    Analytics Safety Rule (Rule #5):
    - When ``confirmed_numeric_cols`` is provided, only compute analytics for those columns.
    - When not provided, apply heuristic: skip columns that look like rank/index
      (small integers 1–100 with high uniqueness) to avoid misleading statistics.
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
        # Analytics safety: skip columns not confirmed as meaningful numeric.
        if confirmed_numeric_cols is not None:
            if k not in confirmed_numeric_cols:
                logger.debug("Analytics skipped for unconfirmed column=%r", k)
                continue
        else:
            # Heuristic guard: skip rank/index-like columns
            if _is_rank_or_index_values(vals):
                logger.debug(
                    "Analytics skipped for rank/index-like column=%r (values look like 1–100 integers)", k
                )
                continue

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
