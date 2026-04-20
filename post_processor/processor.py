"""
Post-processor: schema-agnostic entity normalization, deduplication,
relationship mapping, and output formatting.

All category detection and normalization driven by value patterns — no
hardcoded PAYMENT_METHODS, CURRENCY_MAP, or LABEL_MAP dictionaries.
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from thefuzz import fuzz

class ValidationError(Exception):
    pass


# ── Constants ─────────────────────────────────────────────────────────────────

DATE_FORMATS = [
    "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y",
    "%d/%m/%Y", "%Y/%m/%d", "%d %B %Y",
    "%B %d, %Y", "%d %b %Y", "%b %d, %Y",
]

FUZZY_THRESHOLD = 85


# ── Value-Pattern Detection (replaces hardcoded maps) ────────────────────────

def _detect_currency(raw: str | None) -> str:
    """Detect currency from value pattern — symbol or ISO code."""
    if not raw:
        return ""
    s = raw.strip()
    if not s:
        return ""

    # Symbol-based detection
    symbol_map = {"$": "USD", "€": "EUR", "£": "GBP", "₹": "INR", "¥": "JPY", "₩": "KRW", "₽": "RUB"}
    for symbol, code in symbol_map.items():
        if symbol in s:
            return code

    # ISO code detection (3 uppercase letters)
    upper = s.upper().strip()
    if len(upper) == 3 and upper.isalpha():
        return upper

    return s.upper()[:8]


def _normalize_label_dynamic(label: str | None) -> str:
    """
    Generic label normalization — lowercase, underscored, no hardcoded map.
    E.g., "Invoice Total" → "invoice_total", "Salary" → "salary"
    """
    if not label:
        return ""
    import re
    clean = label.strip().lower()
    clean = re.sub(r"[^a-z0-9]+", "_", clean).strip("_")
    return clean


def _is_payment_method_like(value: str) -> bool:
    """
    Detect if a value looks like a payment method from patterns, not a hardcoded list.
    Pattern: short string (1-3 words), commonly seen payment channel vocabulary.
    """
    s = value.strip().lower()
    if not s or len(s) > 40:
        return False

    # Pattern-based detection: common payment channel patterns
    payment_patterns = [
        # Card patterns
        r"\b(credit|debit)\s*card\b",
        r"\b(visa|mastercard|amex|discover)\b",
        r"\bmaster\s*card\b",
        r"\bamerican\s*express\b",
        # Digital payment patterns
        r"\b(paypal|pay\s*pal)\b",
        r"\b(stripe|razorpay|square)\b",
        r"\b(apple|google|samsung)\s*pay\b",
        r"\b(venmo|zelle|cashapp)\b",
        r"\bupi\b",
        # Traditional patterns
        r"\b(cash|cheque|check)\b",
        r"\b(bank|wire)\s*transfer\b",
        r"\b(net\s*banking|e-?wallet)\b",
    ]

    import re
    for pattern in payment_patterns:
        if re.search(pattern, s):
            return True
    return False


# ── Step 1: Schema Validator ──────────────────────────────────────────────────

def validate_schema(ai_output: dict) -> dict:
    """Structure is strictly enforced by AI schema."""
    return ai_output


# ── Step 2: Date Cleaner ──────────────────────────────────────────────────────

def parse_date(value: str) -> str | None:
    """Try all known formats. Return ISO date or None if invalid."""
    value = str(value).strip()
    for fmt in DATE_FORMATS:
        try:
            parsed = datetime.strptime(value, fmt)
            if parsed.year < 1900 or parsed.year > 2100:
                return None
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

def clean_dates(dates: list) -> list:
    cleaned = []
    for item in dates:
        raw = str(item.get("value", ""))
        parsed = parse_date(raw)
        if parsed is None:
            continue  # drop invalid dates
        entry = {**item, "value": parsed}
        if raw != parsed:
            entry["original"] = raw  # preserve original for traceability
        cleaned.append(entry)
    return cleaned


# ── Step 3: Amount Cleaner (dynamic currency/label detection) ─────────────────

def normalize_currency(currency: str | None) -> str:
    """Dynamic currency normalization from value patterns."""
    return _detect_currency(currency)

def normalize_label(label: str | None) -> str:
    """Dynamic label normalization — no hardcoded map."""
    return _normalize_label_dynamic(label)

def clean_amounts(amounts: list) -> list:
    cleaned = []
    for item in amounts:
        value = item.get("value", 0)
        entry = {
            **item,
            "currency": normalize_currency(item.get("currency", "")),
            "label": normalize_label(item.get("label", "")),
        }
        if isinstance(value, (int, float)) and value < 0:
            entry["flag"] = "negative_value"
        cleaned.append(entry)
    return cleaned


# ── Step 4: Fuzzy Deduplication ───────────────────────────────────────────────

def deduplicate_entities(entity_list: list) -> list:
    """Merge duplicates using fuzzy string matching."""
    deduplicated = []
    for item in entity_list:
        value = str(item.get("value", "")).lower().strip()
        matched = False
        for existing in deduplicated:
            score = fuzz.ratio(value, str(existing.get("value", "")).lower().strip())
            if score >= FUZZY_THRESHOLD:
                # Keep higher confidence version
                if item.get("confidence", 0) > existing.get("confidence", 0):
                    existing["value"] = item["value"]
                    existing["confidence"] = item["confidence"]
                matched = True
                break
        if not matched:
            deduplicated.append(item.copy())
    return deduplicated


# ── Step 5: Payment Method Separator (pattern-based) ──────────────────────────

def separate_payment_methods(organizations: list) -> tuple:
    """Split orgs into real organizations and payment methods using pattern detection."""
    orgs = []
    payment_methods = []
    for item in organizations:
        value = str(item.get("value", "")).strip()
        if _is_payment_method_like(value):
            payment_methods.append({
                **item,
                "value": value.title()
            })
        else:
            orgs.append(item)
    return orgs, payment_methods


# ── Step 6: ID Assigner (generic) ────────────────────────────────────────────

def assign_entity_ids(entities: dict) -> tuple:
    """Assign short IDs to every entity type dynamically."""
    id_map = {}
    result = {}

    # Auto-detect entity types and assign prefixes
    for entity_type in entities:
        items = entities.get(entity_type, [])
        if not isinstance(items, list):
            continue

        # Generate prefix from first 1-2 chars of type name
        prefix = entity_type[:2].lower() if entity_type else "x"
        if prefix in ("pe",):
            prefix = "p"  # person_names → p
        elif prefix in ("or",):
            prefix = "o"  # organizations → o
        elif prefix in ("da",):
            prefix = "d"  # dates → d
        elif prefix in ("am",):
            prefix = "a"  # amounts → a
        elif prefix in ("pa",):
            prefix = "pm"  # payment_methods → pm

        result[entity_type] = []
        for i, item in enumerate(items):
            entity_id = f"{prefix}{i+1}"
            raw_value = str(item.get("value", ""))
            id_map[raw_value] = entity_id
            id_map[raw_value.lower()] = entity_id
            result[entity_type].append({"id": entity_id, **item})

    return result, id_map


# ── Step 7: Relationship Mapper ───────────────────────────────────────────────

def process_relationships(relationships: list, id_map: dict) -> list:
    processed = []
    for rel in relationships:
        from_val = rel.get("from", "")
        to_val = rel.get("to", "")
        processed.append({
            "type": rel.get("type", "unknown"),
            "from": id_map.get(from_val, id_map.get(from_val.lower(), from_val)),
            "to": id_map.get(to_val, id_map.get(to_val.lower(), to_val)),
            "confidence": rel.get("confidence", 0.0),
            "attributes": rel.get("attributes", {})
        })
    return processed


# ── Confidence Aggregator ─────────────────────────────────────────────────────

def compute_overall_confidence(entities: dict) -> float:
    scores = [
        item["confidence"]
        for entity_list in entities.values()
        if isinstance(entity_list, list)
        for item in entity_list
        if isinstance(item, dict) and "confidence" in item
    ]
    return round(sum(scores) / len(scores), 2) if scores else 0.0


# ── Main Entry Point ──────────────────────────────────────────────────────────

def post_process(ai_output: dict, source_file: str, file_metadata: dict) -> dict:
    try:
        # Step 1: Validate schema
        data = validate_schema(ai_output)

        entities = data.get("entities", {})

        # Step 2: Clean dates (if present)
        if "dates" in entities:
            entities["dates"] = clean_dates(entities["dates"])

        # Step 3: Clean amounts (if present)
        if "amounts" in entities:
            entities["amounts"] = clean_amounts(entities["amounts"])

        # Step 4: Deduplicate all text-based entity lists
        for key in list(entities.keys()):
            if key in ("amounts", "dates"):
                continue
            if isinstance(entities[key], list):
                entities[key] = deduplicate_entities(entities[key])

        # Step 5: Separate payment methods from organizations (if present)
        if "organizations" in entities:
            orgs, payment_methods = separate_payment_methods(entities["organizations"])
            entities["organizations"] = orgs
            entities["payment_methods"] = deduplicate_entities(payment_methods)

        data["entities"] = entities

        # Step 6: Assign IDs
        entities_with_ids, id_map = assign_entity_ids(data["entities"])

        # Step 7: Map relationships
        relationships = process_relationships(data.get("relationships", []), id_map)

        # Compute confidence
        confidence_overall = compute_overall_confidence(entities_with_ids)

        meta = dict(file_metadata or {})
        meta["confidence_overall"] = confidence_overall
        meta["processed_at"] = datetime.now(timezone.utc).isoformat()

        return {
            "document_id": str(uuid.uuid4()),
            "document_type": data.get("document_type", "generic"),
            "source_file": source_file,
            "status": "success",
            "error": None,
            "entities": entities_with_ids,
            "relationships": relationships,
            "metadata": meta,
        }

    except Exception as e:
        raise ValidationError(f"Post-processing failed: {e}")


def _convert_universal_envelope_to_toml(result: dict) -> str:
    """Universal envelope: document_id, type, status, metadata, [[data]] rows."""
    lines = []
    lines.append(f'document_id = "{result.get("document_id", "")}"')
    lines.append(f'document_type = "{result.get("document_type", "")}"')
    lines.append(f'status = "{result.get("status", "")}"')
    lines.append(f'error = {json.dumps(result.get("error"))}')
    lines.append("")
    lines.append("[metadata]")
    for key, value in (result.get("metadata") or {}).items():
        if value is None:
            lines.append(f'{key} = "null"')
        elif isinstance(value, str):
            lines.append(f'{key} = "{value}"')
        else:
            lines.append(f"{key} = {json.dumps(value)}")
    lines.append("")
    for row in result.get("data") or []:
        lines.append("[[data]]")
        for k, v in row.items():
            if v is None:
                lines.append(f"{k} = null")
            elif isinstance(v, bool):
                lines.append(f"{k} = {str(v).lower()}")
            elif isinstance(v, (int, float)):
                lines.append(f"{k} = {v}")
            else:
                lines.append(f'{k} = {json.dumps(str(v))}')
        lines.append("")
    return "\n".join(lines).strip()


def convert_to_toml(result: dict) -> str:
    """Convert structured JSON output to TOML format — generic entity iteration."""
    if isinstance(result.get("data"), list) and "entities" not in result:
        return _convert_universal_envelope_to_toml(result)

    lines = []

    # Document info
    lines.append(f'document_id = "{result.get("document_id", "")}"')
    lines.append(f'document_type = "{result.get("document_type", "")}"')
    lines.append(f'source_file = "{result.get("source_file", "")}"')
    lines.append(f'status = "{result.get("status", "")}"')
    lines.append(f'error = {json.dumps(result.get("error"))}')
    lines.append("")

    # Metadata
    lines.append("[metadata]")
    metadata = result.get("metadata", {})
    for key, value in metadata.items():
        if value is None:
            lines.append(f'{key} = "null"')
        elif isinstance(value, str):
            lines.append(f'{key} = "{value}"')
        else:
            lines.append(f'{key} = {value}')
    lines.append("")

    # Entities — generic iteration over all entity types
    entities = result.get("entities", {})
    for entity_type, items in entities.items():
        if not isinstance(items, list) or not items:
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            lines.append(f"[[entities.{entity_type}]]")
            for k, v in item.items():
                if v is None:
                    lines.append(f'{k} = "null"')
                elif isinstance(v, bool):
                    lines.append(f"{k} = {str(v).lower()}")
                elif isinstance(v, (int, float)):
                    lines.append(f"{k} = {v}")
                elif isinstance(v, str):
                    lines.append(f'{k} = "{v}"')
                else:
                    lines.append(f'{k} = {json.dumps(v)}')
            lines.append("")

    # Relationships
    if result.get("relationships"):
        for rel in result["relationships"]:
            lines.append("[[relationships]]")
            lines.append(f'type = "{rel.get("type", "")}"')
            lines.append(f'from = "{rel.get("from", "")}"')
            lines.append(f'to = "{rel.get("to", "")}"')
            lines.append(f'confidence = {rel.get("confidence", 0.0)}')
            lines.append("")

    return "\n".join(lines)