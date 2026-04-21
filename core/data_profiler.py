"""
Dynamic Data Profiler — column classification and dataset understanding.

Classification uses value patterns, distributions, and statistical properties
AND column-name context when available (e.g. column named 'revenue' → monetary).

Column-name context provides INTENT hints that resolve ambiguity:
  - If a column name suggests monetary meaning, treat it as monetary regardless of
    value size (do not classify based on digit count or value magnitude alone).
  - If column names suggest analytical/ranking data, lock dataset type accordingly.

Paradigm: Observe → Infer (values + name context) → Decide → Clean
"""

from __future__ import annotations

import collections
import logging
import math
import re
import statistics
from dataclasses import dataclass, field
from typing import Any, Iterable

from core.cleaning import (
    amount_from_value,
    is_valid_date,
    is_valid_email,
    is_valid_phone,
    normalize_date_value,
)
from parsers.csv_parser import normalize_field_name

logger = logging.getLogger(__name__)

# ── Reserved / internal keys — never profiled ────────────────────────────────

_RESERVED_KEYS = frozenset(
    {
        "confidence",
        "is_anomaly",
        "is_valid_email",
        "is_valid_date",
        "is_valid_numeric",
        "is_outlier",
    }
)

_INTERNAL_KEYS = frozenset({"__imputed__"})

_PIPELINE_ARTIFACT_KEYS = frozenset(
    {
        "__imputed__",
        "imputed",
        "confidence",
        "is_anomaly",
        "is_outlier",
        "is_valid_email",
        "is_valid_date",
        "is_valid_numeric",
    }
)

# ── Pattern regexes ──────────────────────────────────────────────────────────

_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
_PHONE_DIGITS_RE = re.compile(r"\D+")
_BRACKET_NOISE_RE = re.compile(r"\[.*?\]")
_PAREN_NOISE_RE = re.compile(r"\(.*?\)")
_CITATION_MARKER_RE = re.compile(r"[†‡\*\^]+")
_REPEATED_PUNCT_RE = re.compile(r"([!?.,;:]){3,}")
_OCR_ARTIFACT_RE = re.compile(r"[|¦}{~`]{2,}")
_CURRENCY_SYMBOL_RE = re.compile(r"^[\s]*[$€£₹¥₩₽]+[\s]*[\d,]+\.?\d*\s*$")
_COMMA_FORMATTED_RE = re.compile(r"^[\s$€£₹¥₩₽]*\d{1,3}(?:,\d{3})+\.?\d*\s*$")
_DATE_PATTERNS = [
    re.compile(r"^\d{4}-\d{2}-\d{2}$"),                     # ISO
    re.compile(r"^\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}$"),       # US/EU short
    re.compile(r"^\d{1,2}\s+\w+\s+\d{4}$"),                 # 15 March 2024
    re.compile(r"^\w+\s+\d{1,2},?\s+\d{4}$"),               # March 15, 2024
]

# ── Column-name keyword hints ────────────────────────────────────────────────
# These resolve ambiguity when value patterns alone are inconclusive.
# If a column name contains one of these tokens, it provides INTENT context.

_MONETARY_NAME_KEYWORDS = frozenset({
    "revenue", "earnings", "gross", "salary", "cost", "price", "amount",
    "net", "income", "profit", "expense", "fee", "wage", "budget", "total",
    "compensation", "payment", "balance", "spend", "turnover", "margin",
    "receipt", "invoice", "billing", "charge", "premium", "valuation",
})

_ANALYTICAL_NAME_KEYWORDS = frozenset({
    "rank", "ranking", "index", "score", "rating", "metric", "percentile",
    "statistic", "statistics", "average", "median", "benchmark", "kpi",
    "leaderboard", "standings", "position",
})

# Regex to split column names into tokens for whole-word matching.
# Splits on underscores, spaces, hyphens, camelCase boundaries.
_TOKEN_SPLIT_RE = re.compile(r"[_\s\-]+|(?<=[a-z])(?=[A-Z])")


def _name_tokens(name: str) -> set[str]:
    """Split a column name into lowercase tokens for whole-word matching."""
    normalized = normalize_field_name(str(name)).lower()
    return {t for t in _TOKEN_SPLIT_RE.split(normalized) if t}


def _column_name_suggests_monetary(name: str) -> bool:
    """True if the column name contains a whole-word token implying monetary meaning."""
    tokens = _name_tokens(name)
    return bool(tokens & _MONETARY_NAME_KEYWORDS)


def _column_name_suggests_analytical(name: str) -> bool:
    """True if the column name contains a whole-word token implying analytical/rankings data."""
    tokens = _name_tokens(name)
    return bool(tokens & _ANALYTICAL_NAME_KEYWORDS)


def _is_present(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, str) and not v.strip():
        return False
    return True


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


# ═══════════════════════════════════════════════════════════════════════════════
# Column Profile
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class ColumnProfile:
    """Statistical profile of a single column, derived entirely from value patterns."""

    name: str
    inferred_type: str = "text"  # email, phone, date, monetary, identifier, categorical, boolean, numeric, text
    fill_rate: float = 0.0
    distinct_ratio: float = 0.0
    is_candidate_key: bool = False
    allows_imputation: bool = False
    value_pattern: str | None = None
    sample_values: list[Any] = field(default_factory=list)
    statistics: dict[str, Any] = field(default_factory=dict)
    # Semantic confidence flag — False means the type assignment is uncertain.
    # Downstream consumers MUST respect this and fall back to generic handling.
    semantic_confidence_high: bool = True


@dataclass
class DatasetProfile:
    """Profile of the entire dataset, derived from column profiles and relationships."""

    dataset_type: str = "unknown"  # entity, transactional, analytical
    columns: dict[str, ColumnProfile] = field(default_factory=dict)
    candidate_keys: list[str] = field(default_factory=list)
    critical_columns: list[str] = field(default_factory=list)
    dedup_keys: list[str] = field(default_factory=list)
    column_count: int = 0
    row_count: int = 0


# ═══════════════════════════════════════════════════════════════════════════════
# Value-pattern detectors (operate on individual values, no column name input)
# ═══════════════════════════════════════════════════════════════════════════════

def _looks_like_email(v: str) -> bool:
    return bool(_EMAIL_RE.match(v.strip()))


def _looks_like_phone(v: str) -> bool:
    """True if the string is a phone-like sequence of 10-15 digits."""
    s = v.strip()
    digits = _PHONE_DIGITS_RE.sub("", s)
    if 10 <= len(digits) <= 15 and len(digits) >= len(s) * 0.40:
        return True
    return is_valid_phone(s)


def _looks_like_date(v: str) -> bool:
    s = v.strip()
    if len(s) < 4 or len(s) > 40:
        return False
    # SAFETY: bare numeric strings (e.g. "7", "10", "42") are NOT dates.
    # A 4-digit all-numeric string could be a year ("2020") — allow those through,
    # but reject anything shorter that is purely digits (ranks, indices, counts).
    if s.isdigit() and len(s) < 4:
        return False
    # Reject values that are purely numeric with no date separator — e.g. "20201301"
    # Only 8-digit strings matching YYYYMMDD are allowed through the full parse.
    if s.isdigit() and len(s) not in (4, 8):
        return False
    # Quick pattern match first
    for pat in _DATE_PATTERNS:
        if pat.match(s):
            # Extra guard: a 4-digit match (e.g. "2020") is a year, not a full date
            # — allow it only via the full date parser that rejects bare years.
            if s.isdigit() and len(s) == 4:
                return is_valid_date(s)
            return True
    # Full parse fallback
    return is_valid_date(s)


def _looks_like_currency_value(v: str) -> bool:
    """True if value contains currency symbol + number, OR is comma-formatted."""
    s = v.strip()
    if not s:
        return False
    # Must have explicit currency symbol OR comma-formatted number (1,000.00)
    if _CURRENCY_SYMBOL_RE.match(s):
        return True
    if _COMMA_FORMATTED_RE.match(s):
        return True
    return False


def _coerce_number(v: Any) -> float | None:
    """
    Attempt to parse a value as a number.

    Non-destructive: cleans noise (brackets, citations) before parsing
    so "7[2]" → 7, "100†" → 100, "1,500[a]" → 1500.
    """
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        if v != v:  # NaN
            return None
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        if "@" in s:  # Don't try to parse emails as numbers
            return None
        # Try direct parse first
        n = amount_from_value(s)
        if n is not None:
            return float(n)
        # Clean noise and retry: "7[2]" → "7", "100†" → "100"
        cleaned = clean_text_noise(s)
        if cleaned and cleaned != s:
            n2 = amount_from_value(cleaned)
            if n2 is not None:
                return float(n2)
    return None


def _coerce_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if value == 1:
            return True
        if value == 0:
            return False
        return None
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"true", "t", "yes", "y", "1"}:
            return True
        if v in {"false", "f", "no", "n", "0"}:
            return False
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# Column Profiling — classify from value patterns + column name context
# ═══════════════════════════════════════════════════════════════════════════════

def profile_column(name: str, values: list[Any]) -> ColumnProfile:
    """
    Profile a single column from its values AND column name context.

    Column-name context resolves ambiguity:
    - If the name suggests monetary intent (salary, revenue, cost, etc.),
      the column is classified as 'monetary' regardless of value magnitude.
    - Value-pattern analysis is the primary signal; name hints override
      only when values are numeric but the type is ambiguous.
    """
    prof = ColumnProfile(name=name)
    total = len(values)
    if total == 0:
        return prof

    filled_vals = [v for v in values if _is_present(v)]
    filled = len(filled_vals)
    prof.fill_rate = filled / total if total else 0.0

    if filled == 0:
        prof.inferred_type = "text"
        prof.allows_imputation = False
        return prof

    # Collect string representations for pattern analysis
    str_vals = [_safe_str(v) for v in filled_vals]
    prof.sample_values = str_vals[:5]

    # Distinct ratio
    lower_vals = [s.lower() for s in str_vals if s]
    distinct = len(set(lower_vals))
    prof.distinct_ratio = distinct / filled if filled else 0.0

    # ── Count pattern hits ──
    email_hits = sum(1 for s in str_vals if s and _looks_like_email(s))
    phone_hits = sum(1 for s in str_vals if s and _looks_like_phone(s))
    date_hits = sum(1 for s in str_vals if s and _looks_like_date(s))
    bool_hits = sum(1 for v in filled_vals if _coerce_bool(v) is not None)
    currency_hits = sum(1 for s in str_vals if s and _looks_like_currency_value(s))

    num_hits = 0
    num_values: list[float] = []
    for v in filled_vals:
        n = _coerce_number(v)
        if n is not None:
            num_hits += 1
            num_values.append(n)

    # ── Classification by pattern ratios ──

    # Email: 40%+ values contain @ with valid structure
    if filled >= 1 and email_hits / filled >= 0.35:
        prof.inferred_type = "email"
        prof.allows_imputation = False
        return prof

    # Phone: 50%+ values look like phone numbers
    if filled >= 1 and phone_hits / filled >= 0.50:
        prof.inferred_type = "phone"
        prof.allows_imputation = False
        return prof

    # Boolean: 80%+ values are bool-like, or 50%+ if column exhibits binary pattern
    if filled >= 2 and bool_hits / filled >= 0.80:
        prof.inferred_type = "boolean"
        prof.allows_imputation = True
        return prof

    # Date: 60%+ values parse as dates AND column is not purely numeric
    # (avoid classifying year columns like 2020, 2021 as dates)
    num_ratio = num_hits / filled if filled else 0
    if filled >= 2 and date_hits / filled >= 0.60 and num_ratio < 0.50:
        prof.inferred_type = "date"
        prof.allows_imputation = False
        return prof

    # ── Column-name monetary override ─────────────────────────────────────────
    # TYPE CLASSIFICATION RULE: If a column name suggests revenue, earnings,
    # salary, cost, price, or amount — treat it as monetary regardless of
    # value size. Do not classify based on digit count or value magnitude alone.
    name_is_monetary = _column_name_suggests_monetary(name)

    # ── RANK / INDEX GUARD ───────────────────────────────────────────────────
    # Small integers 1–100 are NOT money — UNLESS column name says otherwise.
    if num_hits >= 2 and num_ratio >= 0.55 and not name_is_monetary:
        if _is_rank_or_index_column(num_values, filled):
            prof.inferred_type = "identifier"
            prof.is_candidate_key = False
            prof.allows_imputation = False
            prof.semantic_confidence_high = False
            _add_numeric_stats(prof, num_values)
            return prof

    # Numeric: 55%+ values parse as numbers
    if filled >= 1 and num_ratio >= 0.55:
        # Column name says monetary → classify as monetary (high confidence)
        if name_is_monetary:
            prof.inferred_type = "monetary"
            prof.semantic_confidence_high = True
            prof.allows_imputation = False
            _add_numeric_stats(prof, num_values)
            if num_values and len(num_values) >= 3:
                _decide_numeric_imputation(prof, num_values, filled)
            return prof
        # Value evidence: currency symbols/formatting
        if currency_hits / filled >= 0.60:
            prof.inferred_type = "monetary"
            prof.allows_imputation = False
            _add_numeric_stats(prof, num_values)
            if num_values and len(num_values) >= 3:
                _decide_numeric_imputation(prof, num_values, filled)
            return prof
        prof.inferred_type = "numeric"
        _add_numeric_stats(prof, num_values)
        if num_values and len(num_values) >= 3:
            _decide_numeric_imputation(prof, num_values, filled)
        return prof

    # Categorical vs free text: low cardinality → categorical
    if filled >= 5 and prof.distinct_ratio < 0.50:
        prof.inferred_type = "categorical"
        prof.allows_imputation = True  # mode imputation is safe
        return prof

    # Default: text
    prof.inferred_type = "text"

    # Identifier detection: high cardinality + many distinct values
    if filled >= 5 and prof.distinct_ratio >= 0.90:
        prof.inferred_type = "identifier"
        prof.is_candidate_key = True
        prof.allows_imputation = False
    elif filled >= 3 and prof.distinct_ratio >= 0.90:
        # Smaller datasets — check for structured pattern (codes, IDs)
        digit_heavy = sum(1 for s in str_vals if len(re.sub(r"\D", "", s)) >= 3)
        if digit_heavy / filled >= 0.30:
            prof.inferred_type = "identifier"
            prof.is_candidate_key = True
            prof.allows_imputation = False

    return prof


def _is_rank_or_index_column(num_values: list[float], filled: int) -> bool:
    """
    Return True if the numeric values look like a rank or index column.

    Criteria (ALL must hold):
    - 80%+ of values are integers (no fractional part)
    - 80%+ of values fall within [1, 100]
    - The MINIMUM value is ≤ 10 (real ranks start near 1, e.g., 1st, 2nd, 3rd)
    - The value SPREAD (max - min) is ≤ 30 (ranks span a narrow sequential range)
    - The column has some variation (values not all the same)

    Why the min/spread guards?
    Score columns like [80, 82, 85, 90] are NOT ranks even though they are
    integers in [1, 100]. Their minimum (~80) and spread (~10) distinguish
    them from true rank columns like [1, 2, 3, 4] or [3, 7, 1, 5].

    This prevents score/percentage/grade columns from being labeled as rank/index
    while still catching true positional columns.
    """
    if not num_values or filled < 2:
        return False

    int_vals = [int(v) for v in num_values if v == int(v)]
    integer_ratio = len(int_vals) / len(num_values)

    # Must be mostly integers
    if integer_ratio < 0.80:
        return False

    # Must fall within [1, 100]
    small_int_hits = sum(1 for v in int_vals if 1 <= v <= 100)
    small_int_ratio = small_int_hits / len(num_values)
    if small_int_ratio < 0.80:
        return False

    # CRITICAL: real ranks START near 1 — min value must be ≤ 10.
    # Scores like [80, 82, 85] have min=80 → NOT a rank column.
    min_val = min(int_vals)
    if min_val > 10:
        return False

    # CRITICAL: ranks span a NARROW sequential range — spread must be ≤ 30.
    # This catches true ranking columns (1-10, 2-15) and excludes score/percentage
    # columns that happen to use small integers in a mid-range (e.g. 40-70).
    max_val = max(int_vals)
    if (max_val - min_val) > 30:
        return False

    # Must have some variation (not all the same value)
    distinct_count = len(set(int_vals))
    if distinct_count < 2:
        return False

    return True


def _add_numeric_stats(prof: ColumnProfile, values: list[float]) -> None:
    """Add descriptive statistics for numeric columns."""
    if not values:
        return
    prof.statistics = {
        "count": len(values),
        "mean": statistics.mean(values),
        "median": statistics.median(values),
        "min": min(values),
        "max": max(values),
    }
    if len(values) >= 2:
        prof.statistics["stdev"] = statistics.stdev(values)
    else:
        prof.statistics["stdev"] = 0.0


def _decide_numeric_imputation(prof: ColumnProfile, values: list[float], filled: int) -> None:
    """Decide if a numeric column allows imputation based on variance and uniqueness."""
    distinct_ratio = len(set(round(v, 6) for v in values)) / len(values)

    # Near-unique numerics (IDs, ranks) — never impute
    if distinct_ratio >= 0.90 and len(values) >= 5:
        prof.is_candidate_key = True
        prof.allows_imputation = False
        return

    # Small dataset with all-distinct values — never impute
    if len(values) <= 24 and distinct_ratio >= (len(values) - 1) / len(values):
        prof.allows_imputation = False
        return

    # High coefficient of variation — risky to impute
    mean = prof.statistics.get("mean", 0)
    stdev = prof.statistics.get("stdev", 0)
    if mean != 0 and stdev / abs(mean) > 1.5:
        prof.allows_imputation = False
        return

    # Low variance, enough samples → safe to impute
    prof.allows_imputation = True


# ═══════════════════════════════════════════════════════════════════════════════
# Dataset Profiling
# ═══════════════════════════════════════════════════════════════════════════════

def profile_dataset(records: list[dict[str, Any]]) -> DatasetProfile:
    """
    Profile an entire dataset. Returns type classification, column profiles,
    candidate keys, and cleaning strategy hints.

    Classification uses column relationships, uniqueness, variance — NOT keywords.
    """
    dp = DatasetProfile()
    if not records:
        return dp

    dp.row_count = len(records)

    # Collect all content keys (skip pipeline artifacts)
    all_keys: list[str] = []
    seen_keys: set[str] = set()
    for r in records:
        for k in r.keys():
            if k not in seen_keys and k not in _RESERVED_KEYS and k not in _INTERNAL_KEYS and not str(k).startswith("__"):
                seen_keys.add(k)
                all_keys.append(k)
    dp.column_count = len(all_keys)

    # Profile each column
    for k in all_keys:
        values = [r.get(k) for r in records]
        cp = profile_column(k, values)
        dp.columns[k] = cp

    # Identify candidate keys
    dp.candidate_keys = [
        name for name, cp in dp.columns.items()
        if cp.is_candidate_key
    ]

    # Identify critical columns (high fill-rate OR candidate keys)
    dp.critical_columns = _infer_critical_columns(dp)

    # Identify dedup keys
    dp.dedup_keys = _infer_dedup_keys(dp)

    # Classify dataset type
    dp.dataset_type = _classify_dataset_type(dp)

    logger.info(
        "Dataset profiled | type=%s | columns=%d | rows=%d | keys=%s | critical=%s",
        dp.dataset_type, dp.column_count, dp.row_count,
        dp.candidate_keys[:5], dp.critical_columns[:5],
    )

    return dp


def _infer_critical_columns(profile: DatasetProfile) -> list[str]:
    """
    Infer important columns from statistical properties:
    - candidate keys
    - high fill rate (>= 0.75 and enough rows)
    - mostly numeric with high fill
    - near-unique
    """
    critical: set[str] = set()
    n = profile.row_count

    for name, cp in profile.columns.items():
        # Candidate keys are always critical
        if cp.is_candidate_key:
            critical.add(name)
            continue

        # High fill rate with enough data
        min_filled = max(3, n // 2) if n > 0 else 3
        filled_count = int(cp.fill_rate * n)
        high_fill = cp.fill_rate >= 0.75 and filled_count >= min_filled

        # Near-unique with high fill
        highly_unique = filled_count >= 5 and cp.distinct_ratio >= 0.90

        # Mostly numeric
        mostly_numeric = cp.inferred_type in ("numeric", "monetary") and cp.statistics.get("count", 0) > 0

        if high_fill or highly_unique or mostly_numeric:
            critical.add(name)

    result = sorted(critical)[:16]
    if result:
        logger.info("Critical columns (dynamic): %s", result)
    return result


def _infer_dedup_keys(profile: DatasetProfile) -> list[str]:
    """Auto-detect keys for deduplication from column profiles."""
    # Prefer candidate keys
    keys = list(profile.candidate_keys)

    if not keys:
        # Fall back to identifier-type columns
        keys = [
            name for name, cp in profile.columns.items()
            if cp.inferred_type == "identifier"
        ]

    if not keys:
        # Fall back to highest-cardinality text columns
        text_cols = sorted(
            [(name, cp) for name, cp in profile.columns.items() if cp.inferred_type in ("text", "categorical")],
            key=lambda x: -x[1].distinct_ratio,
        )
        keys = [name for name, _ in text_cols[:2]]

    # Add a date column if available (helps uniqueness)
    date_cols = [name for name, cp in profile.columns.items() if cp.inferred_type == "date"]
    if date_cols and len(keys) < 4:
        keys.extend(date_cols[:1])

    # Fallback: use all content columns
    if not keys:
        keys = list(profile.columns.keys())

    return keys


def _classify_dataset_type(profile: DatasetProfile) -> str:
    """
    Classify dataset as entity | transactional | analytical using
    column names, column relationships, and statistical properties.

    DATASET CLASSIFICATION RULE:
    - Detect dataset type from column names, values, AND structure.
    - If the dataset appears to be analytical (rankings, reports, statistics,
      financials, leaderboards): lock this type and do not reclassify.

    - Entity: has identifiers, text-heavy, profile of people/things
    - Transactional: identifiers + monetary/date, event-oriented
    - Analytical: mostly numeric, metric-heavy, ranking/reporting data
    """
    if not profile.columns:
        return "unknown"

    cols = profile.columns
    total = len(cols)
    if total == 0:
        return "unknown"

    # ── Column-name hint: detect analytical intent from column names ──
    # If column names suggest ranking, statistics, or reporting data,
    # lock the dataset type as analytical immediately.
    analytical_name_count = sum(
        1 for name in cols if _column_name_suggests_analytical(name)
    )
    if analytical_name_count >= 2:
        logger.info(
            "Analytical dataset detected via column names (%d analytical-named columns)",
            analytical_name_count,
        )
        return "analytical"

    # Count column types
    type_counts: dict[str, int] = collections.Counter()
    for cp in cols.values():
        type_counts[cp.inferred_type] += 1

    numeric_count = type_counts.get("numeric", 0) + type_counts.get("monetary", 0)
    text_count = type_counts.get("text", 0) + type_counts.get("categorical", 0)
    identifier_count = type_counts.get("identifier", 0)
    date_count = type_counts.get("date", 0)
    monetary_count = type_counts.get("monetary", 0)
    email_count = type_counts.get("email", 0)
    phone_count = type_counts.get("phone", 0)

    numeric_ratio = numeric_count / total
    text_ratio = text_count / total

    # ── Analytical: majority numeric, few identifiers, metric-heavy ──
    if numeric_ratio >= 0.60 and identifier_count <= 1 and text_count <= 2:
        return "analytical"

    # ── Transactional: has identifiers + monetary + date ──
    if (identifier_count >= 1 or len(profile.candidate_keys) >= 1) and monetary_count >= 1 and date_count >= 1:
        return "transactional"

    # ── Transactional fallback: has date + monetary without explicit identifier ──
    if date_count >= 1 and monetary_count >= 1:
        return "transactional"

    # ── Transactional: identifiers + monetary (no date required) ──
    if (identifier_count >= 1 or len(profile.candidate_keys) >= 1) and monetary_count >= 1:
        return "transactional"

    # ── Entity: text-heavy with identifiers or contact info ──
    if text_ratio >= 0.40 and (email_count >= 1 or phone_count >= 1 or identifier_count >= 1):
        return "entity"

    # ── Entity fallback: text-heavy, moderate identifiers ──
    if text_ratio >= 0.50:
        return "entity"

    # ── Default based on balance ──
    if numeric_ratio >= 0.50:
        return "analytical"

    return "entity"


# ═══════════════════════════════════════════════════════════════════════════════
# Field type detection (backward-compatible interface for final_cleaning)
# ═══════════════════════════════════════════════════════════════════════════════

def detect_field_types(records: list[dict[str, Any]]) -> dict[str, set[str]]:
    """
    Backward-compatible field type detection using the profiler.

    Returns ``{"numeric": set(...), "email": set(...), "date": set(...),
               "text": set(...), "phone": set(...), "boolean": set(...),
               "identifier": set(...), "categorical": set(...), "monetary": set(...)}``.
    """
    out: dict[str, set[str]] = {
        "numeric": set(),
        "email": set(),
        "date": set(),
        "text": set(),
        "phone": set(),
        "boolean": set(),
        "identifier": set(),
        "categorical": set(),
        "monetary": set(),
    }
    if not records:
        return out

    profile = profile_dataset(records)
    for name, cp in profile.columns.items():
        out[cp.inferred_type].add(name)
        # Text columns also go into the text bucket for backward compat
        if cp.inferred_type == "categorical":
            out["text"].add(name)

    return out


# ═══════════════════════════════════════════════════════════════════════════════
# Noise Removal (generic, regex-based)
# ═══════════════════════════════════════════════════════════════════════════════

def clean_text_noise(text: str) -> str:
    """
    Remove dynamically detected noise patterns:
    - Bracket patterns → [...]
    - Parenthesized annotations → (...)
    - Citation markers → †, ‡, *, ^
    - Repeated punctuation → !!! → !
    - OCR artifacts → ||, {}, ~~
    """
    if not text:
        return text
    t = _BRACKET_NOISE_RE.sub("", text)
    t = _PAREN_NOISE_RE.sub("", t)
    t = _CITATION_MARKER_RE.sub("", t)
    t = _REPEATED_PUNCT_RE.sub(r"\1", t)
    t = _OCR_ARTIFACT_RE.sub("", t)
    # Collapse multiple spaces
    t = re.sub(r"\s+", " ", t)
    return t.strip()


# ═══════════════════════════════════════════════════════════════════════════════
# Generic numeric outlier detection (works on any column)
# ═══════════════════════════════════════════════════════════════════════════════

def detect_numeric_outliers(
    records: list[dict[str, Any]],
    column: str,
) -> set[int]:
    """
    Return indices of rows that are statistical outliers for the given column.
    Uses IQR method. Does NOT modify any values.
    """
    vals_with_idx: list[tuple[int, float]] = []
    for i, r in enumerate(records):
        v = r.get(column)
        if isinstance(v, (int, float)) and not isinstance(v, bool) and v == v:
            vals_with_idx.append((i, float(v)))

    if len(vals_with_idx) < 4:
        return set()

    sorted_vals = sorted(v for _, v in vals_with_idx)
    n = len(sorted_vals)
    q1 = sorted_vals[n // 4]
    q3 = sorted_vals[(3 * n) // 4]
    iqr = q3 - q1
    if iqr <= 0:
        iqr = abs(q3) or 1.0
    low = q1 - 1.5 * iqr
    high = q3 + 1.5 * iqr

    outlier_indices: set[int] = set()
    for idx, v in vals_with_idx:
        if v < low or v > high:
            outlier_indices.add(idx)

    return outlier_indices


def compute_median(values: list[float]) -> float | None:
    """Compute median of a list of floats."""
    if not values:
        return None
    s = sorted(values)
    m = len(s) // 2
    if len(s) % 2:
        return float(s[m])
    return float(s[m - 1] + s[m]) / 2.0


def compute_mode(values: list[str]) -> str | None:
    """Compute mode of a list of strings."""
    if not values:
        return None
    counter = collections.Counter(values)
    if not counter:
        return None
    return counter.most_common(1)[0][0]
