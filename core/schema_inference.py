"""
Generic column → semantic role inference using the data profiler.
No hardcoded keyword→role mapping. Delegates to data_profiler for type detection.
"""

from __future__ import annotations

import logging
from typing import Any

from core.data_profiler import profile_column, profile_dataset
from core.intelligence_record import heuristic_intelligence_row, mapped_intelligence_row
from parsers.csv_parser import normalize_field_name

logger = logging.getLogger(__name__)


def infer_mapping_from_columns(
    columns: list[str],
    sample_rows: list[dict[str, Any]] | None = None,
) -> dict[str, list[str]]:
    """
    Build semantic mapping: role -> [original column names].
    Uses data profiler when sample data available, otherwise returns empty mapping.
    """
    from core.semantic_mapping import classify_fields
    return classify_fields(columns, sample_rows=sample_rows)


def heuristic_row_without_mapping(row: dict[str, Any]) -> dict[str, Any]:
    """When no column mapping exists, preserve all fields and infer from values."""
    return heuristic_intelligence_row(row)


def mapping_to_universal_row(
    row: dict[str, Any],
    mapping: dict[str, Any],
    *,
    confidence: float = 0.82,
    schema_source: str = "inferred",
) -> dict[str, Any]:
    """Map columns to semantic roles and merge with preserved fields."""
    _ = confidence  # confidence finalized in universal pipeline
    src = schema_source if schema_source != "inferred" else "heuristic"
    return mapped_intelligence_row(row, mapping, schema_source=src)


def mapping_is_non_empty(mapping: dict[str, Any]) -> bool:
    return bool(mapping) and any(v for v in mapping.values() if v)
