"""
Dataset-agnostic anomaly detection: critical fields, format validation,
and per-column statistical outlier detection.

Uses dynamically detected column types — no hardcoded field names.
"""

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
    *,
    email_columns: set[str] | None = None,
    date_columns: set[str] | None = None,
) -> None:
    """
    Mutates ``is_anomaly`` in place (OR semantics).

    Uses dynamically provided column sets for email/date validation.
    Falls back to checking validation flags if column sets not provided.
    """
    critical_fields = critical_fields or []
    col_means = _numeric_column_means(records)

    for r in records:
        prev = bool(r.get("is_anomaly"))
        bad = False

        if not r.get("is_valid_numeric", True):
            bad = True
            logger.debug("Anomaly | invalid_numeric")

        # Check email columns dynamically
        if email_columns:
            for ek in email_columns:
                ev = r.get(ek)
                if ev is not None and str(ev).strip():
                    if not is_valid_email(str(ev)):
                        bad = True
                        logger.debug("Anomaly | invalid_email in column=%s", ek)
                        break
        elif r.get("is_valid_email") is False:
            bad = True
            logger.debug("Anomaly | invalid_email (from flag)")

        # Check date columns dynamically
        if date_columns:
            for dk in date_columns:
                dv = r.get(dk)
                if dv is not None and str(dv).strip():
                    if not is_valid_date(dv):
                        bad = True
                        logger.debug("Anomaly | invalid_date in column=%s", dk)
                        break
        elif r.get("is_valid_date") is False:
            bad = True
            logger.debug("Anomaly | invalid_date (from flag)")

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
                        k, fv, round(mean, 4),
                    )
                    break

        r["is_anomaly"] = prev or bad
