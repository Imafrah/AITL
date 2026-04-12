"""Row-level anomalies: invalid values, missing critical context, amount spikes (dataset-aware)."""

from __future__ import annotations

from typing import Any

from core.cleaning import is_valid_date, is_valid_numeric


def apply_anomaly_detection(
    records: list[dict[str, Any]],
    dataset_type: str | None = None,
) -> None:
    """Mutates ``is_anomaly`` in place (OR semantics)."""
    dt = (dataset_type or "generic").lower()

    amounts: list[float] = []
    for r in records:
        a = r.get("amount")
        if isinstance(a, (int, float)) and not isinstance(a, bool) and a == a:
            amounts.append(float(a))

    avg_amt = sum(amounts) / len(amounts) if amounts else 0.0

    for r in records:
        prev = bool(r.get("is_anomaly"))

        invalid_num = not r.get("is_valid_numeric", True)
        date_bad = False
        if r.get("date") is not None and str(r.get("date")).strip():
            date_bad = not is_valid_date(r.get("date"))

        missing_critical = False
        if dt == "employee":
            missing_critical = not (
                r.get("person_name") or r.get("name") or r.get("email")
            )
        elif dt in ("invoice", "sales", "transaction"):
            missing_critical = r.get("amount") is None and r.get("quantity") is None
        else:
            missing_critical = not (
                r.get("person_name") or r.get("name") or r.get("email") or r.get("phone")
            ) and r.get("amount") is None

        spike = False
        if avg_amt > 0 and r.get("amount") is not None:
            try:
                spike = float(r["amount"]) > 2.0 * avg_amt
            except (TypeError, ValueError):
                spike = True

        r["is_anomaly"] = prev or invalid_num or date_bad or missing_critical or spike
