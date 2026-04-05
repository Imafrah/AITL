import uuid
from datetime import datetime, timezone
from thefuzz import fuzz

class ValidationError(Exception):
    pass

def clean_llm_response(raw: str) -> str:
    cleaned = raw.strip()
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        cleaned = "\n".join(lines).strip()
    return cleaned

def normalize_label(label: str) -> str:
    """Normalize AI-generated labels into standard vocabulary."""
    label_map = {
        "amount": "invoice_total",
        "total": "invoice_total",
        "price": "invoice_total",
        "fee": "invoice_total",
        "payment": "invoice_total",
    }
    return label_map.get(label.lower(), label.lower())

def deduplicate_entities(entity_list: list) -> list:
    """Merge duplicate entities using fuzzy string matching."""
    deduplicated = []
    for item in entity_list:
        matched = False
        for existing in deduplicated:
            score = fuzz.ratio(
                item["value"].lower().strip(),
                existing["value"].lower().strip()
            )
            if score >= 85:
                if item.get("confidence", 0) > existing.get("confidence", 0):
                    existing["value"] = item["value"]
                    existing["confidence"] = item["confidence"]
                matched = True
                break
        if not matched:
            deduplicated.append(item.copy())
    return deduplicated

def assign_entity_ids(entities: dict) -> dict:
    id_map = {}
    result = {}
    counters = {"person_names": "p", "organizations": "o", "dates": "d", "amounts": "a"}

    for entity_type, prefix in counters.items():
        items = entities.get(entity_type, [])
        items = deduplicate_entities(items)  
        result[entity_type] = []
        for i, item in enumerate(items):
            entity_id = f"{prefix}{i+1}"
            id_map[item.get("value", "")] = entity_id
            enriched = {"id": entity_id, **item}
            if entity_type == "amounts" and "label" in enriched:
                enriched["label"] = normalize_label(enriched["label"])
            result[entity_type].append(enriched)

    return result, id_map

def process_relationships(relationships: list, id_map: dict) -> list:
    """Replace raw string references with entity IDs."""
    processed = []
    for rel in relationships:
        processed.append({
            "type": rel.get("type", "unknown"),
            "from": id_map.get(rel.get("from", ""), rel.get("from", "")),
            "to": id_map.get(rel.get("to", ""), rel.get("to", "")),
            "confidence": rel.get("confidence", 0.0),
            "attributes": rel.get("attributes", {})
        })
    return processed

def compute_overall_confidence(entities: dict) -> float:
    """Average confidence across all entities."""
    all_scores = []
    for entity_list in entities.values():
        for entity in entity_list:
            if "confidence" in entity:
                all_scores.append(entity["confidence"])
    if not all_scores:
        return 0.0
    return round(sum(all_scores) / len(all_scores), 2)

def post_process(ai_output: dict, source_file: str, file_metadata: dict) -> dict:
    try:
        raw_entities = ai_output.get("entities", {})
        raw_relationships = ai_output.get("relationships", [])
        document_type = ai_output.get("document_type", "unknown")

        # Assign IDs to entities
        entities_with_ids, id_map = assign_entity_ids(raw_entities)

        # Process relationships using ID map
        relationships = process_relationships(raw_relationships, id_map)

        # Compute overall confidence
        confidence_overall = compute_overall_confidence(entities_with_ids)

        return {
            "document_id": str(uuid.uuid4()),
            "document_type": document_type,
            "source_file": source_file,
            "status": "success",
            "error": None,
            "entities": entities_with_ids,
            "relationships": relationships,
            "metadata": {
                "file_type": file_metadata.get("file_type"),
                "page_count": file_metadata.get("page_count"),
                "word_count": file_metadata.get("word_count"),
                "confidence_overall": confidence_overall,
                "processed_at": datetime.now(timezone.utc).isoformat()
            }
        }

    except Exception as e:
        raise ValidationError(f"Post-processing failed: {e}")