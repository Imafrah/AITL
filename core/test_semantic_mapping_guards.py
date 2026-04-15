from __future__ import annotations

from core.intelligence_record import semantic_intelligence_row
from core.semantic_mapping import classify_fields, merge_field_maps


def test_classify_fields_skips_validation_columns() -> None:
    cols = ["is_valid_email", "is_valid_date", "confidence", "transaction_id", "city"]
    mapping = classify_fields(cols)
    flat = {c for v in mapping.values() for c in v}
    assert "is_valid_email" not in flat
    assert "is_valid_date" not in flat
    assert "confidence" not in flat


def test_merge_field_maps_filters_invalid_and_unknown_overlay_columns() -> None:
    base = {"city": ["City"]}
    overlay = {
        "email": ["is_valid_email", "Email"],
        "date": ["is_valid_date"],
    }
    merged = merge_field_maps(base, overlay, valid_columns={"City", "Email"})
    assert merged.get("email") == ["Email"]
    assert "date" not in merged or merged["date"] == []


def test_semantic_row_does_not_create_email_from_non_email_values() -> None:
    row = {"is_valid_email": False, "customer_flag": True}
    field_map = {"email": ["is_valid_email"]}
    rec = semantic_intelligence_row(row, field_map)
    assert "email" not in rec
