"""Row-level anomaly flags: salary spike vs cohort and missing identity signals."""

from __future__ import annotations

from typing import Any


def apply_anomaly_detection(records: list[dict[str, Any]]) -> None:
    """Mutates ``is_anomaly`` in place (OR semantics with any prior flag)."""
    salaries: list[float] = []
    for r in records:
        v = r.get("salary")
        if isinstance(v, (int, float)) and not isinstance(v, bool) and v == v:
            salaries.append(float(v))

    avg = sum(salaries) / len(salaries) if salaries else 0.0

    for r in records:
        prev = bool(r.get("is_anomaly"))
        missing_id = not (r.get("name") or r.get("email") or r.get("phone"))
        spike = False
        if avg > 0 and r.get("salary") is not None:
            try:
                sv = float(r["salary"])
                spike = sv > 2.0 * avg
            except (TypeError, ValueError):
                pass
        r["is_anomaly"] = prev or missing_id or spike
