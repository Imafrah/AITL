import json
import uuid
from datetime import datetime, timezone
from thefuzz import fuzz

class ValidationError(Exception):
    pass


# ── Constants ─────────────────────────────────────────────────────────────────

PAYMENT_METHODS = {
    "paypal", "pay pal", "credit card", "creditcard", "credit_card",
    "debit card", "cash", "visa", "mastercard", "master card",
    "american express", "amex", "bank transfer", "wire transfer",
    "stripe", "razorpay", "upi"
}

CURRENCY_MAP = {
    "$": "USD", "usd": "USD",
    "€": "EUR", "eur": "EUR",
    "£": "GBP", "gbp": "GBP",
    "₹": "INR", "inr": "INR",
    "¥": "JPY", "jpy": "JPY",
}

LABEL_MAP = {
    # Invoice labels
    "amount": "invoice_total",
    "total": "invoice_total",
    "price": "invoice_total",
    "fee": "invoice_total",
    "payment": "invoice_total",
    "subtotal": "subtotal",
    "tax": "tax",
    "discount": "discount",
    # Employee labels
    "salary": "salary",
    "wage": "salary",
    "compensation": "salary",
    "bonus": "bonus",
    # Generic
    "revenue": "revenue",
    "expense": "expense",
    "cost": "cost",
}

DATE_FORMATS = [
    "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y",
    "%d/%m/%Y", "%Y/%m/%d", "%d %B %Y",
    "%B %d, %Y", "%d %b %Y", "%b %d, %Y",
]

FUZZY_THRESHOLD = 85


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


# ── Step 3: Amount Cleaner ────────────────────────────────────────────────────

def normalize_currency(currency: str | None) -> str:
    raw = (currency or "").strip()
    if not raw:
        return ""
    key = raw.lower()
    return CURRENCY_MAP.get(key, raw.upper())

def normalize_label(label: str | None) -> str:
    raw = (label or "").strip().lower()
    if not raw:
        return ""
    return LABEL_MAP.get(raw, raw)

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


# ── Step 5: Payment Method Separator ─────────────────────────────────────────

def separate_payment_methods(organizations: list) -> tuple:
    """Split orgs into real organizations and payment methods."""
    orgs = []
    payment_methods = []
    for item in organizations:
        value = str(item.get("value", "")).strip().lower()
        if value in PAYMENT_METHODS:
            payment_methods.append({
                **item,
                "value": item["value"].strip().title()
            })
        else:
            orgs.append(item)
    return orgs, payment_methods


# ── Step 6: ID Assigner ───────────────────────────────────────────────────────

def assign_entity_ids(entities: dict) -> tuple:
    """Assign short IDs to every entity. Return enriched entities + id_map."""
    id_map = {}
    result = {}

    prefixes = {
        "person_names": "p",
        "organizations": "o",
        "dates": "d",
        "amounts": "a",
        "payment_methods": "pm",
    }

    for entity_type, prefix in prefixes.items():
        items = entities.get(entity_type, [])
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
        for item in entity_list
        if "confidence" in item
    ]
    return round(sum(scores) / len(scores), 2) if scores else 0.0


# ── Main Entry Point ──────────────────────────────────────────────────────────

def post_process(ai_output: dict, source_file: str, file_metadata: dict) -> dict:
    try:
        # Step 1: Validate schema
        data = validate_schema(ai_output)

        # Step 2: Clean dates
        data["entities"]["dates"] = clean_dates(data["entities"]["dates"])

        # Step 3: Clean amounts
        data["entities"]["amounts"] = clean_amounts(data["entities"]["amounts"])

        # Step 4: Deduplicate person names and organizations
        data["entities"]["person_names"] = deduplicate_entities(
            data["entities"]["person_names"]
        )
        data["entities"]["organizations"] = deduplicate_entities(
            data["entities"]["organizations"]
        )

        # Step 5: Separate payment methods from organizations
        orgs, payment_methods = separate_payment_methods(
            data["entities"]["organizations"]
        )
        data["entities"]["organizations"] = orgs
        data["entities"]["payment_methods"] = deduplicate_entities(payment_methods)

        # Step 6: Assign IDs
        entities_with_ids, id_map = assign_entity_ids(data["entities"])

        # Step 7: Map relationships
        relationships = process_relationships(data["relationships"], id_map)

        # Compute confidence
        confidence_overall = compute_overall_confidence(entities_with_ids)

        meta = dict(file_metadata or {})
        meta["confidence_overall"] = confidence_overall
        meta["processed_at"] = datetime.now(timezone.utc).isoformat()

        return {
            "document_id": str(uuid.uuid4()),
            "document_type": data["document_type"],
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
    """Convert structured JSON output to TOML format."""
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

    # Entities
    entities = result.get("entities", {})

    if entities.get("person_names"):
        for item in entities["person_names"]:
            lines.append("[[entities.person_names]]")
            lines.append(f'id = "{item.get("id", "")}"')
            lines.append(f'value = "{item.get("value", "")}"')
            lines.append(f'confidence = {item.get("confidence", 0.0)}')
            lines.append("")

    if entities.get("organizations"):
        for item in entities["organizations"]:
            lines.append("[[entities.organizations]]")
            lines.append(f'id = "{item.get("id", "")}"')
            lines.append(f'value = "{item.get("value", "")}"')
            lines.append(f'confidence = {item.get("confidence", 0.0)}')
            lines.append("")

    if entities.get("dates"):
        for item in entities["dates"]:
            lines.append("[[entities.dates]]")
            lines.append(f'id = "{item.get("id", "")}"')
            lines.append(f'value = "{item.get("value", "")}"')
            if item.get("label"):
                lines.append(f'label = "{item.get("label", "")}"')
            lines.append(f'confidence = {item.get("confidence", 0.0)}')
            lines.append("")

    if entities.get("amounts"):
        for item in entities["amounts"]:
            lines.append("[[entities.amounts]]")
            lines.append(f'id = "{item.get("id", "")}"')
            lines.append(f'value = {item.get("value", 0)}')
            lines.append(f'currency = "{item.get("currency", "")}"')
            lines.append(f'label = "{item.get("label", "")}"')
            lines.append(f'confidence = {item.get("confidence", 0.0)}')
            if item.get("flag"):
                lines.append(f'flag = "{item.get("flag", "")}"')
            lines.append("")

    if entities.get("payment_methods"):
        for item in entities["payment_methods"]:
            lines.append("[[entities.payment_methods]]")
            lines.append(f'id = "{item.get("id", "")}"')
            lines.append(f'value = "{item.get("value", "")}"')
            lines.append(f'confidence = {item.get("confidence", 0.0)}')
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