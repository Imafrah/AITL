"""Dataset-agnostic anomalies: critical fields, formats, and per-column 3× mean outliers."""

from __future__ import annotations

import logging
from typing import Any

from core.cleaning import is_valid_date, is_valid_email

logger = logging.getLogger(__name__)

_RESERVED = frozenset(
    {
        "confidence",
        "is_anomaly",
        "is_valid_email",
        "is_valid_date",
        "is_valid_numeric",
    }
)


def _numeric_column_means(records: list[dict[str, Any]]) -> dict[str, float]:
    buckets: dict[str, list[float]] = {}
    for r in records:
        for k, v in r.items():
            if k in _RESERVED or k.startswith("_"):
                continue
            if isinstance(v, (int, float)) and not isinstance(v, bool) and v == v:
                buckets.setdefault(k, []).append(float(v))
    return {k: sum(vals) / len(vals) for k, vals in buckets.items() if vals}


def apply_anomaly_detection(
    records: list[dict[str, Any]],
    critical_fields: list[str] | None = None,
) -> None:
    """Mutates ``is_anomaly`` in place (OR semantics)."""
    critical_fields = critical_fields or []
    col_means = _numeric_column_means(records)

    for r in records:
        prev = bool(r.get("is_anomaly"))
        bad = False

        if not r.get("is_valid_numeric", True):
            bad = True
            logger.debug("Anomaly | invalid_numeric")

        if r.get("email") is not None and str(r.get("email")).strip():
            if not is_valid_email(r.get("email")):
                bad = True
                logger.debug("Anomaly | invalid_email")

        if r.get("date") is not None and str(r.get("date")).strip():
            if not is_valid_date(r.get("date")):
                bad = True
                logger.debug("Anomaly | invalid_date")

        if critical_fields:
            for cf in critical_fields:
                v = r.get(cf)
                if v is None or (isinstance(v, str) and not str(v).strip()):
                    bad = True
                    logger.debug("Anomaly | missing_critical field=%s", cf)
                    break

        if not bad:
            for k, mean in col_means.items():
                if mean <= 0:
                    continue
                v = r.get(k)
                if not isinstance(v, (int, float)) or isinstance(v, bool):
                    continue
                try:
                    fv = float(v)
                except (TypeError, ValueError):
                    continue
                if fv > 3.0 * mean:
                    bad = True
                    logger.debug(
                        "Anomaly | outlier column=%s value=%s mean=%s",
                        k,
                        fv,
                        round(mean, 4),
                    )
                    break

        r["is_anomaly"] = prev or bad
