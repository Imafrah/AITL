"""
AI-powered schema detection — open-ended column type classification.

The model classifies columns by VALUE PATTERNS, not fixed semantic roles.
Returns a flexible mapping dict, not a rigid Pydantic model with hardcoded fields.
"""

import json
import logging
import time
from typing import Any

from google import genai
from google.genai import types
from pydantic import BaseModel, ConfigDict, Field

from ai_layer.extractor import is_retryable_api_error, remove_additional_properties

logger = logging.getLogger(__name__)


class SchemaDetectionError(Exception):
    pass


class SchemaMappingModel(BaseModel):
    """Open-ended semantic roles → CSV column names (exact spelling from data)."""

    model_config = ConfigDict(extra="allow")

    # Core value-pattern-based roles (not fixed schema fields)
    person_name: list[str] = Field(default_factory=list)
    email: list[str] = Field(default_factory=list)
    phone: list[str] = Field(default_factory=list)
    location: list[str] = Field(default_factory=list)
    organization: list[str] = Field(default_factory=list)
    date: list[str] = Field(default_factory=list)
    amount: list[str] = Field(default_factory=list)
    currency: list[str] = Field(default_factory=list)
    identifier: list[str] = Field(default_factory=list)
    category: list[str] = Field(default_factory=list)


class SchemaAIDetection(BaseModel):
    model_config = ConfigDict(extra="ignore")

    schema_type: str = Field(
        default="generic",
        description="Dataset classification: entity, transactional, analytical, or generic",
    )
    mapping: SchemaMappingModel = Field(default_factory=SchemaMappingModel)


def detect_schema_ai(sample_rows: list[dict], api_key: str) -> dict:
    """
    Ask the model to classify columns by VALUE PATTERNS.
    Returns flexible mapping that callers validate against actual columns.
    """
    if not api_key or not str(api_key).strip():
        raise SchemaDetectionError("API key is required for AI schema detection.")

    # JSON-serializable sample (no numpy / datetime surprises)
    safe_sample: list[dict[str, Any]] = []
    for row in sample_rows[:8]:
        safe_row: dict[str, Any] = {}
        for k, v in row.items():
            if v is None:
                safe_row[str(k)] = None
            elif isinstance(v, (str, int, float, bool)):
                safe_row[str(k)] = v
            else:
                safe_row[str(k)] = str(v)
        safe_sample.append(safe_row)

    try:
        client = genai.Client(api_key=api_key)
        schema_dict = SchemaAIDetection.model_json_schema()
        clean_schema = remove_additional_properties(schema_dict)

        prompt = f"""You are a data analyst. Analyze the columns in these CSV rows by examining their VALUE PATTERNS.

Given these CSV rows as JSON (column names are exact — reuse them in mapping values):

1. Classify the dataset type based on column relationships:
   - "entity": describes people/things (has names, contact info, identifiers)
   - "transactional": describes events (has dates, amounts, IDs, status)
   - "analytical": mostly numeric data/metrics/statistics
   - "generic": doesn't fit above categories

2. Map each column to its detected ROLE based on value patterns:
   - person_name: columns where values look like human names (2+ capitalized words)
   - email: columns where values contain @ and domain patterns
   - phone: columns with 10-15 digit numeric sequences
   - location: columns with city/region/country names
   - organization: columns with company/department/institution names
   - date: columns where values are parseable as dates
   - amount: columns with monetary or significant numeric values
   - currency: columns with ISO currency codes or symbols
   - identifier: columns with unique codes/IDs/serial numbers
   - category: columns with low-cardinality text values (statuses, types)

   Leave a list empty if no column matches that role.
   Use EXACT column names from the data.

Rows:
{json.dumps(safe_sample, indent=2, default=str)}
"""

        delays = (1.0, 2.0, 4.0)
        max_attempts = len(delays) + 1
        for attempt in range(max_attempts):
            try:
                response = client.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        temperature=0.1,
                        response_mime_type="application/json",
                        response_schema=clean_schema,
                    ),
                )

                raw = getattr(response, "text", None) or ""
                if not str(raw).strip():
                    raise SchemaDetectionError("Empty response from schema detection model.")

                data = json.loads(raw)
                parsed = SchemaAIDetection.model_validate(data)
                mapping_dump = parsed.mapping.model_dump()
                # Drop empty lists so callers can check `if mapping`
                mapping = {k: v for k, v in mapping_dump.items() if v}
                return {"schema_type": parsed.schema_type.strip().lower(), "mapping": mapping}
            except SchemaDetectionError:
                raise
            except Exception as e:
                if attempt < max_attempts - 1 and is_retryable_api_error(e):
                    wait = delays[attempt]
                    logger.warning(
                        "Retrying schema detection (attempt %s) after %ss: %s",
                        attempt + 1,
                        wait,
                        e,
                    )
                    time.sleep(wait)
                    continue
                raise SchemaDetectionError(f"Schema detection failed: {e}") from e

    except SchemaDetectionError:
        raise
    except Exception as e:
        raise SchemaDetectionError(f"Schema detection failed: {e}") from e
