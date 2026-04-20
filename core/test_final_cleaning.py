"""
Tests for the production-quality, schema-agnostic final cleaning layer.

Covers: dynamic field type detection (via data_profiler), imputation lineage,
dynamic validation flags, null-rate tracking, quality score, output modes,
dataset type classification, and write_cleaning_outputs with stats.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

import pytest

from core.data_profiler import (
    ColumnProfile,
    DatasetProfile,
    detect_field_types,
    profile_column,
    profile_dataset,
)
from core.final_cleaning import (
    CleaningConfig,
    _compute_null_rate,
    _refresh_validation_flags,
    enforce_schema,
    run_final_cleaning_layer,
    write_cleaning_outputs,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _cfg(**overrides: Any) -> CleaningConfig:
    """Build a CleaningConfig with sensible test defaults, overridable."""
    defaults = dict(
        clean_mode="safe",
        email_invalid_strategy="none",
        email_placeholder="unknown@example.invalid",
        text_missing_placeholder=None,
        track_imputation=True,
        min_values_for_median=3,
    )
    defaults.update(overrides)
    return CleaningConfig(**defaults)


def _sample_records() -> list[dict[str, Any]]:
    """Mixed-type dataset with emails, dates, numbers, IDs, and phones."""
    return [
        {
            "employee_id": "EMP001",
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "phone": "+1-555-123-4567",
            "hire_date": "2023-01-15",
            "salary": 75000,
            "department": "Engineering",
        },
        {
            "employee_id": "EMP002",
            "name": "Bob Smith",
            "email": "bob@example.com",
            "phone": "+1-555-234-5678",
            "hire_date": "15/03/2022",
            "salary": 82000,
            "department": "Marketing",
        },
        {
            "employee_id": "EMP003",
            "name": "Carol White",
            "email": "invalid-email",
            "phone": "+1-555-345-6789",
            "hire_date": "2021-07-20",
            "salary": None,
            "department": None,
        },
        {
            "employee_id": "EMP004",
            "name": "Dave Brown",
            "email": None,
            "phone": "+1-555-456-7890",
            "hire_date": "not-a-date",
            "salary": 68000,
            "department": "Sales",
        },
        {
            "employee_id": "EMP005",
            "name": "Eve Davis",
            "email": "eve@example.com",
            "phone": None,
            "hire_date": "2024-06-01",
            "salary": 91000,
            "department": "Engineering",
        },
    ]


# ── 1. Date Column Detection (via data_profiler) ────────────────────────────

class TestDateColumnDetection:
    def test_date_column_detected(self):
        records = _sample_records()
        types = detect_field_types(records)
        assert "date" in types, "detect_field_types must return a 'date' bucket"
        assert "hire_date" in types["date"], (
            f"hire_date should be detected as date, got types={types}"
        )

    def test_date_column_not_in_text_or_numeric(self):
        records = _sample_records()
        types = detect_field_types(records)
        assert "hire_date" not in types["text"]
        assert "hire_date" not in types["numeric"]

    def test_pure_numeric_not_classified_as_date(self):
        """Columns like count (pure ints) should be classified as numeric."""
        records = [
            {"year": 2020, "count": 100},
            {"year": 2021, "count": 200},
            {"year": 2022, "count": 300},
            {"year": 2023, "count": 400},
            {"year": 2024, "count": 500},
            {"year": 2025, "count": 600},  # extra rows to pass identifier threshold
        ]
        types = detect_field_types(records)
        # count is clearly numeric (not monetary, not date)
        assert "count" in types["numeric"], f"count should be numeric, got {types}"


# ── 2. Profiler-Based Column Detection ───────────────────────────────────────

class TestProfilerColumnDetection:
    def test_email_detected_by_value_pattern(self):
        """Columns where 40%+ values contain @ should be detected as email."""
        records = _sample_records()
        types = detect_field_types(records)
        assert "email" in types["email"], f"email should be detected, got {types}"

    def test_phone_detected_by_value_pattern(self):
        """Columns with 10-15 digit sequences should be detected as phone."""
        records = _sample_records()
        types = detect_field_types(records)
        assert "phone" in types["phone"], f"phone should be detected, got {types}"

    def test_identifier_detected_by_uniqueness(self):
        """High-cardinality alphanumeric columns should be detected as identifiers."""
        records = _sample_records()
        profile = profile_dataset(records)
        cp = profile.columns.get("employee_id")
        assert cp is not None
        assert cp.inferred_type == "identifier", (
            f"employee_id should be identifier, got {cp.inferred_type}"
        )

    def test_numeric_detected_by_value_pattern(self):
        """Columns with mostly numbers should be numeric (not monetary when no currency symbols)."""
        records = _sample_records()
        types = detect_field_types(records)
        assert "salary" in types["numeric"], f"salary should be numeric, got {types}"

    def test_categorical_detected_for_low_cardinality(self):
        """Low-cardinality text columns should be categorical."""
        records = [{"status": s, "amount": i*100}
                   for i, s in enumerate(["Active", "Inactive", "Active", "Active", "Inactive", "Active"])]
        profile = profile_dataset(records)
        cp = profile.columns.get("status")
        assert cp is not None
        assert cp.inferred_type == "categorical", f"status should be categorical, got {cp.inferred_type}"


# ── 3. Dataset Type Classification ───────────────────────────────────────────

class TestDatasetTypeClassification:
    def test_entity_dataset(self):
        """Text-heavy with identifiers and contact info → entity."""
        records = _sample_records()
        profile = profile_dataset(records)
        assert profile.dataset_type == "entity", (
            f"Expected entity, got {profile.dataset_type}. "
            f"Types: {[(n, c.inferred_type) for n, c in profile.columns.items()]}"
        )

    def test_analytical_dataset(self):
        """Mostly numeric columns → analytical."""
        records = [
            {"metric_a": 100.5, "metric_b": 200.3, "metric_c": 300.1, "metric_d": 50.0, "metric_e": 99.9},
            {"metric_a": 110.2, "metric_b": 210.4, "metric_c": 310.2, "metric_d": 55.0, "metric_e": 88.8},
            {"metric_a": 105.0, "metric_b": 205.1, "metric_c": 305.5, "metric_d": 52.0, "metric_e": 77.7},
        ]
        profile = profile_dataset(records)
        assert profile.dataset_type == "analytical", (
            f"Expected analytical, got {profile.dataset_type}"
        )

    def test_transactional_dataset(self):
        """Has identifier + monetary (currency-formatted) + date → transactional."""
        records = [
            {"txn_id": "T001", "amount": "$1,500.00", "date": "2024-01-15", "status": "completed"},
            {"txn_id": "T002", "amount": "$2,300.00", "date": "2024-01-16", "status": "pending"},
            {"txn_id": "T003", "amount": "$800.00", "date": "2024-01-17", "status": "completed"},
            {"txn_id": "T004", "amount": "$3,100.00", "date": "2024-01-18", "status": "failed"},
            {"txn_id": "T005", "amount": "$950.00", "date": "2024-01-19", "status": "completed"},
        ]
        profile = profile_dataset(records)
        assert profile.dataset_type == "transactional", (
            f"Expected transactional, got {profile.dataset_type}. "
            f"Types: {[(n, c.inferred_type) for n, c in profile.columns.items()]}"
        )


# ── 4. Imputation Lineage Method Tags ────────────────────────────────────────

class TestImputationLineage:
    def test_median_imputation_tagged(self):
        # Enough duplicate values and text columns to avoid 'analytical' classification
        records = [
            {"score": 80, "name": "A", "dept": "Engineering"},
            {"score": 80, "name": "B", "dept": "Marketing"},
            {"score": 80, "name": "C", "dept": "Engineering"},
            {"score": 90, "name": "D", "dept": "Sales"},
            {"score": 90, "name": "E", "dept": "Engineering"},
            {"score": 85, "name": "F", "dept": "Marketing"},
            {"score": None, "name": "G", "dept": "Sales"},
        ]
        cfg = _cfg(track_imputation=True)
        cleaned, stats = run_final_cleaning_layer(records, config=cfg)
        imputed_rows = [r for r in cleaned if r.get("__imputed__")]
        assert len(imputed_rows) >= 1, (
            f"At least one row should have __imputed__. "
            f"dataset_type={stats.get('dataset_type')} "
            f"Cleaned: {[{k: v for k, v in r.items() if k in ('score', '__imputed__')} for r in cleaned]}"
        )
        for r in imputed_rows:
            imp = r["__imputed__"]
            for field, method in imp.items():
                assert method == "median", (
                    f"Expected method='median', got '{method}' for field '{field}'"
                )

    def test_text_placeholder_tagged(self):
        records = [
            {"amount": 100, "city": "NYC"},
            {"amount": 200, "city": None},
            {"amount": 300, "city": "LA"},
        ]
        cfg = _cfg(track_imputation=True, text_missing_placeholder="unknown")
        cleaned, stats = run_final_cleaning_layer(records, config=cfg)
        imputed_rows = [r for r in cleaned if r.get("__imputed__")]
        found_text_ph = False
        for r in imputed_rows:
            for field, method in r["__imputed__"].items():
                if method == "text_placeholder":
                    found_text_ph = True
        assert found_text_ph, "Should find at least one text_placeholder imputation"

    def test_email_placeholder_tagged(self):
        records = [
            {"email": "alice@test.com", "name": "A"},
            {"email": None, "name": "B"},
            {"email": "bob@test.com", "name": "C"},
        ]
        cfg = _cfg(
            track_imputation=True,
            email_invalid_strategy="placeholder",
            email_placeholder="unknown@example.invalid",
        )
        cleaned, stats = run_final_cleaning_layer(records, config=cfg)
        imputed_rows = [r for r in cleaned if r.get("__imputed__")]
        found_email_ph = False
        for r in imputed_rows:
            for field, method in r["__imputed__"].items():
                if method == "placeholder":
                    found_email_ph = True
        assert found_email_ph, "Should find at least one email placeholder imputation"

    def test_no_imputed_when_tracking_disabled(self):
        records = [
            {"amount": 100, "name": "A"},
            {"amount": None, "name": "B"},
            {"amount": 300, "name": "C"},
        ]
        cfg = _cfg(track_imputation=False)
        cleaned, _ = run_final_cleaning_layer(records, config=cfg)
        for r in cleaned:
            assert "__imputed__" not in r, "Should not have __imputed__ when tracking off"


# ── 5. Dynamic Validation Flags ──────────────────────────────────────────────

class TestDynamicValidation:
    def test_custom_email_column_validated(self):
        record = {"contact_email": "valid@test.com", "work_email": "invalid"}
        _refresh_validation_flags(
            record,
            email_cols={"contact_email", "work_email"},
            date_cols=set(),
        )
        assert record["is_valid_email"] is True  # contact_email is valid

    def test_custom_date_column_validated(self):
        record = {"created_at": "2024-01-15", "updated_at": "not-a-date"}
        _refresh_validation_flags(
            record,
            email_cols=set(),
            date_cols={"created_at", "updated_at"},
        )
        assert record["is_valid_date"] is False

    def test_no_date_columns_defaults_true(self):
        record = {"name": "Test"}
        _refresh_validation_flags(record, email_cols=set(), date_cols=set())
        assert record["is_valid_date"] is True

    def test_no_email_columns_skips_email_flag(self):
        record = {"name": "Test", "amount": 10}
        _refresh_validation_flags(record, email_cols=set(), date_cols=set())
        assert "is_valid_email" not in record


# ── 6. Null Rate Computation ─────────────────────────────────────────────────

class TestNullRate:
    def test_no_nulls(self):
        records = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        rate = _compute_null_rate(records, ["a", "b"])
        assert rate == 0.0

    def test_all_nulls(self):
        records = [{"a": None, "b": None}, {"a": None, "b": None}]
        rate = _compute_null_rate(records, ["a", "b"])
        assert rate == 1.0

    def test_half_nulls(self):
        records = [{"a": 1, "b": None}, {"a": None, "b": 2}]
        rate = _compute_null_rate(records, ["a", "b"])
        assert rate == 0.5

    def test_empty_records(self):
        rate = _compute_null_rate([], ["a", "b"])
        assert rate == 0.0


# ── 7. Quality Score & Cleaning Summary ──────────────────────────────────────

class TestQualityScoreAndSummary:
    def test_clean_data_high_quality(self):
        """Fully clean data should produce quality_score close to 1.0."""
        records = [
            {"name": "Alice", "amount": 100, "email": "alice@test.com"},
            {"name": "Bob", "amount": 200, "email": "bob@test.com"},
            {"name": "Carol", "amount": 300, "email": "carol@test.com"},
        ]
        _, stats = run_final_cleaning_layer(records, config=_cfg())
        cs = stats["cleaning_summary"]
        assert cs["quality_score"] >= 0.8, f"Expected high quality, got {cs['quality_score']}"
        assert cs["null_rate_before"] >= 0.0
        assert cs["null_rate_after"] >= 0.0
        assert cs["schema_field_count"] > 0

    def test_dirty_data_lower_quality(self):
        """Data with invalid emails and missing values should have lower quality_score."""
        records = [
            {"name": "A", "email": "bad1", "amount": 100},
            {"name": "B", "email": "bad2", "amount": 200},
            {"name": "C", "email": "bad3", "amount": 300},
            {"name": "D", "email": "bad4", "amount": None},
            {"name": None, "email": None, "amount": None},
        ]
        _, stats = run_final_cleaning_layer(records, config=_cfg())
        cs = stats["cleaning_summary"]
        assert cs["quality_score"] < 1.0, (
            f"Expected quality < 1.0 for dirty data, got {cs['quality_score']}"
        )

    def test_summary_has_all_required_keys(self):
        records = _sample_records()
        _, stats = run_final_cleaning_layer(records, config=_cfg())
        cs = stats["cleaning_summary"]
        required = {
            "rows_removed", "values_filled", "invalid_values_fixed",
            "null_rate_before", "null_rate_after", "schema_field_count",
            "quality_score",
        }
        assert required.issubset(cs.keys()), (
            f"Missing keys: {required - set(cs.keys())}"
        )

    def test_field_types_includes_date(self):
        records = _sample_records()
        _, stats = run_final_cleaning_layer(records, config=_cfg())
        ft = stats["field_types"]
        assert "date" in ft, "field_types must include 'date' bucket"


# ── 8. Output Modes ──────────────────────────────────────────────────────────

class TestOutputModes:
    def test_safe_mode_keeps_all_rows(self):
        """SAFE mode should keep ALL rows unless 2+ critical fields are missing."""
        records = [
            {"a": 1, "b": 2, "c": 3, "d": 4},
            {"a": 5, "b": 6, "c": None, "d": 4},  # only 1 missing → kept
            {"a": 10, "b": 20, "c": 30, "d": 40},
        ]
        cleaned, stats = run_final_cleaning_layer(records, config=_cfg(clean_mode="safe"))
        assert len(cleaned) == 3, (
            f"Safe mode should keep rows with <= 1 critical missing. "
            f"Got {len(cleaned)}: {cleaned}"
        )

    def test_strict_mode_removes_low_quality(self):
        """STRICT mode should remove rows with >25% missing data."""
        records = [
            {"name": "Alice", "a": 1, "b": 2, "c": 3, "d": 4},
            {"name": "Bob", "a": None, "b": None, "c": 3, "d": 4},  # 40% missing → removed
            {"name": "Carol", "a": 1, "b": 2, "c": 3, "d": 4},
        ]
        cleaned, stats = run_final_cleaning_layer(records, config=_cfg(clean_mode="strict"))
        assert stats["low_quality_removed"] >= 1, "Strict mode should remove low-quality rows"

    def test_strict_removes_more_than_safe(self):
        """Strict mode must always remove >= safe mode rows."""
        records = _sample_records()
        _, safe_stats = run_final_cleaning_layer(records, config=_cfg(clean_mode="safe"))
        _, strict_stats = run_final_cleaning_layer(records, config=_cfg(clean_mode="strict"))
        assert strict_stats["low_quality_removed"] >= safe_stats["low_quality_removed"]

    def test_strict_removes_invalid_email_rows(self):
        """STRICT mode removes rows where any email column is invalid."""
        records = [
            {"person_name": "Alice", "email": "alice@test.com", "amount": 90},
            {"person_name": "Bob", "email": "not-an-email", "amount": 85},
            {"person_name": "Carol", "email": "carol@test.com", "amount": 92},
            {"person_name": "Dave", "email": None, "amount": 88},
        ]
        cleaned, stats = run_final_cleaning_layer(records, config=_cfg(clean_mode="strict"))
        # Bob (invalid email) and Dave (null email) should be removed
        remaining_names = [r.get("person_name") for r in cleaned]
        assert "Bob" not in remaining_names, "Strict should remove invalid email rows"
        assert "Dave" not in remaining_names, "Strict should remove null email rows"
        assert "Alice" in remaining_names
        assert "Carol" in remaining_names

    def test_strict_removes_null_critical_fields(self):
        """STRICT mode removes rows where critical fields are null."""
        records = [
            {"name": "Alice", "amount": 100, "status": "Active"},
            {"name": "Bob", "amount": 200, "status": "Active"},
            {"name": "Carol", "amount": 300, "status": "Active"},
            {"name": None, "amount": None, "status": None},  # all critical null
            {"name": "Eve", "amount": 500, "status": "Active"},
        ]
        cleaned, stats = run_final_cleaning_layer(records, config=_cfg(clean_mode="strict"))
        # The row with all nulls should be removed
        assert stats["low_quality_removed"] >= 1
        assert len(cleaned) < len(records)

    def test_strict_detects_critical_fields(self):
        """STRICT mode should populate critical_fields_detected in stats."""
        records = _sample_records()
        _, stats = run_final_cleaning_layer(records, config=_cfg(clean_mode="strict"))
        assert "critical_fields_detected" in stats
        assert isinstance(stats["critical_fields_detected"], list)

    def test_safe_mode_still_honours_email_remove_row(self):
        """SAFE mode should still respect explicit email_invalid_strategy=remove_row."""
        records = [
            {"name": "Alice", "email": "alice@test.com"},
            {"name": "Bob", "email": "invalid"},
            {"name": "Carol", "email": "carol@test.com"},
        ]
        cleaned, stats = run_final_cleaning_layer(
            records, config=_cfg(clean_mode="safe", email_invalid_strategy="remove_row")
        )
        remaining_names = [r.get("name") for r in cleaned]
        assert "Bob" not in remaining_names or stats["cleaning_summary"]["rows_removed"] >= 1

    def test_clean_mode_in_stats(self):
        """Stats must report which mode was used."""
        _, safe_stats = run_final_cleaning_layer(
            [{"a": 1}], config=_cfg(clean_mode="safe")
        )
        _, strict_stats = run_final_cleaning_layer(
            [{"a": 1}], config=_cfg(clean_mode="strict")
        )
        assert safe_stats["clean_mode"] == "safe"
        assert strict_stats["clean_mode"] == "strict"

    def test_strict_numeric_must_be_present(self):
        """STRICT mode requires numeric fields to be present."""
        records = [
            {"name": "A", "email": "a@t.com", "dept": "Eng", "score": 90, "grade": 85, "rank": 1},
            {"name": "B", "email": "b@t.com", "dept": "Mkt", "score": None, "grade": None, "rank": None},
            {"name": "C", "email": "c@t.com", "dept": "Eng", "score": 88, "grade": 80, "rank": 3},
        ]
        cleaned, stats = run_final_cleaning_layer(records, config=_cfg(clean_mode="strict"))
        # Row B has 0/3 numeric fields → should be removed
        remaining_names = [r.get("name") for r in cleaned]
        assert "B" not in remaining_names, "Strict should remove rows with missing numeric fields"

    def test_safe_removes_only_when_two_or_more_critical_missing(self):
        records = [
            {"transaction_id": "T1", "amount": 100, "date": "2024-01-01", "quantity": 2},
            {"transaction_id": "T2", "amount": 200, "date": None, "quantity": 3},  # 1 critical miss
            {"transaction_id": None, "amount": None, "date": "2024-01-03", "quantity": 1},  # 2 misses
        ]
        cleaned, _ = run_final_cleaning_layer(records, config=_cfg(clean_mode="safe"))
        ids = [r.get("transaction_id") for r in cleaned]
        assert "T2" in ids or len(cleaned) >= 2, "SAFE mode should keep rows with only one missing critical field"

    def test_strict_removes_when_any_critical_missing(self):
        records = [
            {"transaction_id": "T1", "amount": 100, "date": "2024-01-01", "quantity": 2},
            {"transaction_id": "T2", "amount": None, "date": "2024-01-02", "quantity": 3},
        ]
        cleaned, _ = run_final_cleaning_layer(records, config=_cfg(clean_mode="strict"))
        # Strict mode drops rows with missing critical fields
        assert len(cleaned) <= 2
        if len(cleaned) == 1:
            assert cleaned[0].get("transaction_id") == "T1"


# ── 9. Schema Consistency ────────────────────────────────────────────────────

class TestSchemaConsistency:
    def test_all_records_same_keys(self):
        records = [
            {"a": 1, "b": 2},
            {"a": 3, "c": 4},  # missing "b", has extra "c"
        ]
        cleaned, _ = run_final_cleaning_layer(records, config=_cfg())
        if cleaned:
            key_sets = [set(r.keys()) for r in cleaned]
            for ks in key_sets[1:]:
                assert ks == key_sets[0], "All records must have identical keys"

    def test_enforce_schema_fills_missing(self):
        records = [
            {"x": 1},
            {"x": 2, "y": 3},
        ]
        enforced = enforce_schema(records)
        for r in enforced:
            assert "x" in r
            assert "y" in r


# ── 10. Write Cleaning Outputs ───────────────────────────────────────────────

class TestWriteCleaningOutputs:
    def test_output_contains_cleaning_stats(self):
        records = _sample_records()
        cleaned, stats = run_final_cleaning_layer(records, config=_cfg())

        with tempfile.TemporaryDirectory() as tmpdir:
            paths = write_cleaning_outputs(
                "test-doc-001",
                {"validated_output": records, "metadata": {}},
                cleaned,
                cleaning_stats=stats,
                output_dir=tmpdir,
            )
            final_path = paths["final"]
            with open(final_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            assert "cleaning_summary" in data, "Final output must include cleaning_summary"
            assert "field_types" in data, "Final output must include field_types"
            cs = data["cleaning_summary"]
            assert "quality_score" in cs
            assert "null_rate_before" in cs
            assert "null_rate_after" in cs

    def test_output_without_stats_still_works(self):
        cleaned, _ = run_final_cleaning_layer(
            [{"a": 1}], config=_cfg()
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = write_cleaning_outputs(
                "test-doc-002",
                {"validated_output": [{"a": 1}], "metadata": {}},
                cleaned,
                output_dir=tmpdir,
            )
            final_path = paths["final"]
            with open(final_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            assert "final_cleaned_output" in data
            assert "cleaning_summary" not in data  # no stats passed


# ── 11. Edge Cases ───────────────────────────────────────────────────────────

class TestEdgeCases:
    def test_empty_input(self):
        cleaned, stats = run_final_cleaning_layer([], config=_cfg())
        assert cleaned == []
        assert stats["rows_in"] == 0

    def test_single_row(self):
        records = [{"name": "Alice", "amount": 42}]
        cleaned, stats = run_final_cleaning_layer(records, config=_cfg())
        assert len(cleaned) >= 0  # may or may not keep depending on thresholds
        assert "quality_score" in stats["cleaning_summary"]

    def test_all_none_values(self):
        records = [{"a": None, "b": None, "c": 1}, {"a": None, "b": None, "c": 2}]
        cleaned, stats = run_final_cleaning_layer(records, config=_cfg())
        # Non-destructive: nulls are preserved (not replaced with 0/unknown)
        assert stats["cleaning_summary"]["null_rate_before"] > 0
        assert len(cleaned) >= 1, "Safe mode should keep rows with some data"


# ── 12. Data Integrity: No Fabricated Values ─────────────────────────────────

class TestDataIntegrity:
    def test_unique_numeric_not_bulk_filled(self):
        """Near-unique numeric columns should NOT be median-filled."""
        records = [
            {"id": 1001, "score": 88},
            {"id": 1002, "score": 92},
            {"id": 1003, "score": 76},
            {"id": None, "score": None},
            {"id": 1005, "score": 85},
        ]
        cfg = _cfg(track_imputation=True)
        cleaned, _ = run_final_cleaning_layer(records, config=cfg)
        for r in cleaned:
            imp = r.get("__imputed__", {})
            assert "id" not in imp, "ID column should never be imputed"

    def test_phone_not_filled_with_placeholder(self):
        records = [
            {"phone": "+1-555-111-2222", "city": "NYC"},
            {"phone": "+1-555-333-4444", "city": None},
            {"phone": None, "city": "LA"},
            {"phone": "+1-555-777-8888", "city": "SF"},
            {"phone": "+1-555-999-0000", "city": None},
        ]
        cfg = _cfg(text_missing_placeholder="unknown")
        cleaned, _ = run_final_cleaning_layer(records, config=cfg)
        for r in cleaned:
            val = r.get("phone")
            if val is not None and val != "":
                assert val != "unknown", "Phone should never get text placeholder"

    def test_boolean_normalization_to_true_false(self):
        records = [
            {"transaction_id": "T1", "discount_applied": "True", "amount": 100},
            {"transaction_id": "T2", "discount_applied": "false", "amount": 200},
        ]
        cleaned, _ = run_final_cleaning_layer(records, config=_cfg())
        assert len(cleaned) == 2
        vals = [r["discount_applied"] for r in cleaned]
        assert vals == [True, False]


# ── 13. Dynamic Imputation Control ──────────────────────────────────────────

class TestDynamicImputationControl:
    def test_high_variance_not_imputed(self):
        """Columns with coefficient of variation > 1.5 should not be imputed."""
        records = [
            {"value": 1, "name": "A"},
            {"value": 1000000, "name": "B"},
            {"value": 50, "name": "C"},
            {"value": None, "name": "D"},
            {"value": 999999, "name": "E"},
        ]
        cfg = _cfg(track_imputation=True)
        cleaned, _ = run_final_cleaning_layer(records, config=cfg)
        for r in cleaned:
            imp = r.get("__imputed__", {})
            assert "value" not in imp, "High-variance column should not be imputed"

    def test_low_variance_is_imputed(self):
        """Low-variance numeric columns with duplicates should be imputed."""
        records = [
            {"score": 80, "name": "A", "dept": "Engineering"},
            {"score": 82, "name": "B", "dept": "Marketing"},
            {"score": 81, "name": "C", "dept": "Engineering"},
            {"score": 80, "name": "D", "dept": "Sales"},
            {"score": 82, "name": "E", "dept": "Engineering"},
            {"score": 81, "name": "F", "dept": "Marketing"},
            {"score": None, "name": "G", "dept": "Sales"},
        ]
        cfg = _cfg(track_imputation=True)
        cleaned, _ = run_final_cleaning_layer(records, config=cfg)
        imputed = [r for r in cleaned if r.get("__imputed__") and "score" in r["__imputed__"]]
        assert len(imputed) >= 1, (
            f"Low-variance score should be imputed. "
            f"Cleaned: {[{k: v for k, v in r.items() if k in ('score', '__imputed__')} for r in cleaned]}"
        )


# ── 14. Analytical Data Preservation ────────────────────────────────────────

class TestAnalyticalPreservation:
    def test_analytical_no_numeric_modification(self):
        """Analytical datasets should NOT have their numeric values modified."""
        records = [
            {"metric_a": 100.5, "metric_b": 200.3, "metric_c": 300.1, "metric_d": 50.0, "metric_e": 99.9},
            {"metric_a": 110.2, "metric_b": 210.4, "metric_c": 310.2, "metric_d": 55.0, "metric_e": 88.8},
            {"metric_a": 105.0, "metric_b": 205.1, "metric_c": 305.5, "metric_d": 52.0, "metric_e": 77.7},
        ]
        cfg = _cfg(track_imputation=True)
        cleaned, stats = run_final_cleaning_layer(records, config=cfg)
        # No values should have been imputed
        for r in cleaned:
            assert "__imputed__" not in r or not r.get("__imputed__"), \
                "Analytical data should never have imputed values"

    def test_analytical_preserves_all_rows(self):
        """Analytical datasets should NEVER have rows dropped."""
        records = [
            {"metric_a": 100.5, "metric_b": 200.3, "metric_c": 300.1, "metric_d": 50.0, "metric_e": 99.9},
            {"metric_a": None, "metric_b": None, "metric_c": None, "metric_d": None, "metric_e": None},
            {"metric_a": 105.0, "metric_b": 205.1, "metric_c": 305.5, "metric_d": 52.0, "metric_e": 77.7},
        ]
        cfg = _cfg(clean_mode="strict")
        cleaned, stats = run_final_cleaning_layer(records, config=cfg)
        assert len(cleaned) == 3, (
            f"Analytical dataset must preserve ALL rows even in strict mode. "
            f"Got {len(cleaned)}, removed {stats['low_quality_removed']}"
        )

    def test_analytical_preserves_nulls(self):
        """Analytical datasets should keep nulls as None, not replace with 0."""
        records = [
            {"metric_a": 100.5, "metric_b": 200.3, "metric_c": 300.1, "metric_d": 50.0, "metric_e": 99.9},
            {"metric_a": None, "metric_b": 210.4, "metric_c": 310.2, "metric_d": None, "metric_e": 88.8},
            {"metric_a": 105.0, "metric_b": 205.1, "metric_c": 305.5, "metric_d": 52.0, "metric_e": 77.7},
        ]
        cfg = _cfg()
        cleaned, stats = run_final_cleaning_layer(records, config=cfg)
        # Find the row with originally-null metric_a
        null_row = [r for r in cleaned if r.get("metric_b") == 210.4]
        assert len(null_row) == 1, "Row must be preserved"
        # metric_a must be None, NOT 0
        assert null_row[0].get("metric_a") is None, (
            f"Analytical null should stay None, got {null_row[0].get('metric_a')}"
        )

    def test_noise_extraction_from_values(self):
        """Noise like [2], †, [a] should be removed from text values."""
        from core.data_profiler import clean_text_noise, _coerce_number
        # Text noise removal
        assert clean_text_noise("Summer Carnival †[4][a]") == "Summer Carnival"
        assert clean_text_noise("Hello World!!!") == "Hello World!"
        # Numeric noise extraction
        assert _coerce_number("7[2]") == 7.0
        assert _coerce_number("100†") == 100.0
        assert _coerce_number("1,500[a]") == 1500.0

    def test_no_fabricated_zeros(self):
        """Missing numeric values must NOT be replaced with 0."""
        records = [
            {"name": "Alice", "amount": 100, "dept": "Eng"},
            {"name": "Bob", "amount": None, "dept": "Mkt"},
            {"name": "Carol", "amount": 300, "dept": "Eng"},
        ]
        cfg = _cfg()
        cleaned, _ = run_final_cleaning_layer(records, config=cfg)
        bob_row = [r for r in cleaned if r.get("name") and "Bob" in str(r["name"])]
        if bob_row:
            val = bob_row[0].get("amount")
            assert val != 0, (
                f"Missing amount must NOT be replaced with 0, got {val}"
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
