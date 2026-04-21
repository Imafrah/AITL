import json
import logging
import time
from typing import Any, Dict

from google import genai
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class AIServiceError(Exception):
    pass


def _is_retryable_transport_error(exc: BaseException) -> bool:
    """True for 429 / 503 style failures that may succeed after backoff."""
    codes: set[int] = set()
    seen: list[BaseException] = []
    e: BaseException | None = exc
    while e is not None and e not in seen:
        seen.append(e)
        code = getattr(e, "status_code", None)
        if isinstance(code, int):
            codes.add(code)
        code = getattr(e, "code", None)
        if isinstance(code, int):
            codes.add(code)
        resp = getattr(e, "response", None)
        if resp is not None:
            sc = getattr(resp, "status_code", None)
            if isinstance(sc, int):
                codes.add(sc)
        e = getattr(e, "__cause__", None) or getattr(e, "__context__", None)

    if codes & {429, 503}:
        return True

    msg = str(exc).upper()
    if "429" in msg or "503" in msg:
        return True
    if "RESOURCE_EXHAUSTED" in msg or "UNAVAILABLE" in msg or "RATE LIMIT" in msg:
        return True

    return False


# Public alias for other AI callers (e.g. schema detection) that share retry policy.
is_retryable_api_error = _is_retryable_transport_error

# ── Pydantic Schemas ──────────────────────────────────────────────────────────

class EntityBase(BaseModel):
    value: str
    confidence: float = Field(ge=0.0, le=1.0)

class DateEntity(EntityBase):
    label: str

class AmountEntity(BaseModel):
    value: float
    currency: str
    label: str
    confidence: float = Field(ge=0.0, le=1.0)

class Entities(BaseModel):
    person_names: list[EntityBase] = []
    organizations: list[EntityBase] = []
    dates: list[DateEntity] = []
    amounts: list[AmountEntity] = []

class Relationship(BaseModel):
    type: str
    from_field: str = Field(alias="from", default="")
    to: str = Field(default="")
    confidence: float = Field(ge=0.0, le=1.0, default=0.0)
    attributes: Dict[str, Any] = Field(default_factory=dict)

class DocumentExtraction(BaseModel):
    document_type: str
    entities: Entities
    relationships: list[Relationship] = []

# ── Prompt Registry ───────────────────────────────────────────────────────────

PROMPTS = {
    "invoice": """
You are a precise invoice data extraction engine.
Extract these fields:
- Vendor/supplier name (organizations)
- Customer/buyer name (person_names or organizations)
- Invoice number, invoice date, due date (dates with labels)
- Line items, subtotal, tax, total amount (amounts with labels)
- Payment method if mentioned (organizations)

Confidence scoring:
- 1.0 = explicitly stated
- 0.85-0.95 = clearly implied
- 0.70-0.84 = inferred
- below 0.70 = uncertain

Dates must be in YYYY-MM-DD format where possible.
Amount values must be numeric only.

DOCUMENT:
{text}
""",

    "employee_record": """
You are a precise HR data extraction engine.
Extract these fields:
- Employee full names (person_names)
- Department names (organizations) — normalize misspellings:
  Finanace → Finance, Markting → Marketing, HRR → HR, Fin → Finance
- Dates of birth, joining dates (dates with labels)
- Salary and bonus amounts (amounts, label as "salary" or "bonus")

Confidence scoring:
- 1.0 = explicitly stated
- 0.85-0.95 = clearly implied
- 0.70-0.84 = inferred
- below 0.70 = uncertain

IMPORTANT RULES:
- Skip employees with negative or non-numeric ages
- Skip invalid dates (e.g. month 13, day 32, Feb 30)
- Salary values above 500000 are likely outliers — still extract but label as "salary_outlier"
- Do NOT duplicate names — if same name appears multiple times, extract once
- Dates must be in YYYY-MM-DD format

DOCUMENT:
{text}
""",

    "financial_report": """
You are a precise financial data extraction engine.
Extract these fields:
- Account holders, customers (person_names)
- Banks, institutions, companies (organizations)
- Transaction dates, statement dates (dates with labels)
- Debit amounts, credit amounts, balances (amounts with labels)

Confidence scoring:
- 1.0 = explicitly stated
- 0.85-0.95 = clearly implied
- 0.70-0.84 = inferred
- below 0.70 = uncertain

IMPORTANT RULES:
- Payment methods (PayPal, Credit Card, Cash, Visa, Mastercard) are NOT organizations
- Negative amounts = debits, label as "debit"
- Positive amounts = credits, label as "credit"
- Normalize currency: $ → USD, € → EUR, £ → GBP, ₹ → INR
- Dates must be in YYYY-MM-DD format

DOCUMENT:
{text}
""",

    "contract": """
You are a precise legal document extraction engine.
Extract these fields:
- Parties involved (person_names and organizations)
- Contract start date, end date, signing date (dates with labels)
- Contract value, penalty amounts, payment terms (amounts with labels)

Confidence scoring:
- 1.0 = explicitly stated
- 0.85-0.95 = clearly implied
- 0.70-0.84 = inferred
- below 0.70 = uncertain

Dates must be in YYYY-MM-DD format.
Amount values must be numeric only.

DOCUMENT:
{text}
""",

    "tabular": """
You are a data cleaning engine inside AITL (AI Data Translation Layer).
Your job is to clean tabular datasets and return structured JSON.

STEP 1 — READ THE FULL DATASET FIRST
Before making any decision, read every column name and every value
together as a whole. Understand what real-world subject this dataset
describes. Use that understanding for every decision below.

STEP 2 — CLASSIFY DATASET TYPE
Based on your understanding from Step 1, classify as one of:
- analytical  : rankings, leaderboards, statistics, financial summaries,
                performance metrics, reports
- transactional: purchases, events, logs, individual transactions
- entity      : people, products, places with descriptive attributes

Lock this classification. Never override it later.

STEP 3 — CLASSIFY EACH COLUMN
For every column, infer its semantic type using ONLY:
- the column name
- what the column means in the context of this specific dataset
- the pattern of values across the entire column

Allowed column types: monetary, numeric, text, date, identifier, boolean

Rules:
- Use column name as the primary signal
- Use the dataset subject to resolve ambiguity
- Any column that semantically means money, revenue, earnings, gross,
  cost, salary, price, or amount → classify as monetary
- Never classify a column as phone unless its name or values
  explicitly represent a telephone contact number
- Never classify a column as email unless it represents an email address
- Do not use digit count or value magnitude to decide type

STEP 4 — CLEAN EACH VALUE BASED ON ITS COLUMN TYPE

monetary:
  - Strip: currency symbols, commas, spaces
  - Strip: citation markers like [1] [2] [a] [b]
  - Strip: footnote symbols like † ‡ * and trailing letters in brackets
  - Return as: integer or float — NEVER as a string
  - The returned number must be numerically identical to the original
  - Only formatting characters are removed, the number itself is untouched

numeric:
  - Strip: citation markers, footnote symbols, non-numeric characters
  - Return as: integer or float
  - Preserve original magnitude exactly

text:
  - Strip: citation markers like [1] [a], footnote symbols † ‡ *
  - Trim whitespace
  - Preserve everything else exactly

identifier:
  - Strip: citation markers like [1] [a], footnote symbols † ‡ *
  - Trim whitespace
  - Preserve the rest exactly as-is

date:
  - Normalize to YYYY-MM-DD only if value is clearly a date
  - If ambiguous, treat as identifier

boolean:
  - Normalize yes/no, Y/N, 1/0, T/F → true/false only

STEP 5 — ROW DELETION POLICY
Delete a row ONLY IF every field that carries meaning for this
dataset's purpose is missing, null, or completely unreadable.

Never delete rows for:
- missing optional fields
- partial nulls
- low confidence scores
- anomaly flags
- citation-only fields that become null after cleaning

STEP 6 — NULL RATE RULE
Never convert a non-null value into null unless the only content
in that field was noise (citations, symbols) and nothing remains.
Null rate after cleaning must be equal to or less than before cleaning.

STEP 7 — OUTPUT FORMAT
Return a JSON object with this exact structure:
{
  "dataset_type": "<analytical|transactional|entity>",
  "field_types": {
    "<column_name>": "<type>",
    ...
  },
  "cleaned_data": [
    { ...all original fields with cleaned values and correct types... },
    ...
  ]
}

Rules for output:
- Include ALL rows that have at least one meaningful field
- monetary and numeric values must be integer or float, never string
- Preserve original field names exactly
- Do not add new fields
- Do not invent, impute, or guess any values
- Do not include pipeline-internal flags like is_anomaly, confidence,
  is_valid_email, is_valid_date in cleaned_data

DATASET:
{text}
""",

    "unknown": """
You are a precise data extraction engine.
Extract whatever structured information you can find:
- People mentioned (person_names)
- Organizations mentioned (organizations)
- Dates mentioned (dates)
- Monetary amounts (amounts)

Confidence scoring:
- 1.0 = explicitly stated
- 0.85-0.95 = clearly implied
- 0.70-0.84 = inferred
- below 0.70 = uncertain

Dates must be in YYYY-MM-DD format where possible.
Amount values must be numeric only.

DOCUMENT:
{text}
"""
}

# ── Schema Sanitization ───────────────────────────────────────────────────────

def remove_additional_properties(schema):
    """Recursively strip 'additionalProperties' since Gemini API rejects it."""
    if isinstance(schema, dict):
        schema.pop("additionalProperties", None)
        for value in schema.values():
            remove_additional_properties(value)
    elif isinstance(schema, list):
        for item in schema:
            remove_additional_properties(item)
    return schema


# ── Main Extractor ────────────────────────────────────────────────────────────

def extract_entities(text: str, api_key: str, document_type: str = "unknown") -> dict:
    from google.genai import types

    client = genai.Client(api_key=api_key)

    prompt_template = PROMPTS.get(document_type, PROMPTS["unknown"])
    prompt = prompt_template.format(text=text)

    # Tabular cleaning returns free-form JSON, not the DocumentExtraction schema
    if document_type == "tabular":
        backoffs = (1, 2, 4)
        max_retries = 3
        last_error = None
        response = None

        for attempt in range(max_retries + 1):
            try:
                response = client.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        temperature=0.1,
                        response_mime_type="application/json",
                    ),
                )
                break
            except Exception as e:
                last_error = e
                if attempt < max_retries and _is_retryable_transport_error(e):
                    wait_s = backoffs[attempt]
                    logger.warning("Retrying tabular AI call (attempt %s)... after %ss | %s", attempt + 1, wait_s, e)
                    time.sleep(wait_s)
                    continue
                raise AIServiceError(f"AI service failed: {e}") from e

        if response is None and last_error is not None:
            raise AIServiceError(f"AI service failed after retries: {last_error}") from last_error

        raw = getattr(response, "text", None) or ""
        if not str(raw).strip():
            raise AIServiceError("Empty response from AI model.")

        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            raise AIServiceError(f"Invalid JSON from AI:\n{raw[:300]}") from e

    schema_dict = DocumentExtraction.model_json_schema()
    clean_schema = remove_additional_properties(schema_dict)

    backoffs = (1, 2, 4)
    max_retries = 3
    last_error: BaseException | None = None
    response = None

    for attempt in range(max_retries + 1):
        try:
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.2,
                    response_mime_type="application/json",
                    response_schema=clean_schema,
                ),
            )
            break
        except AIServiceError:
            raise
        except Exception as e:
            last_error = e
            if attempt < max_retries and _is_retryable_transport_error(e):
                wait_s = backoffs[attempt]
                logger.warning(
                    "Retrying AI call (attempt %s)... after %ss | %s",
                    attempt + 1,
                    wait_s,
                    e,
                )
                time.sleep(wait_s)
                continue
            raise AIServiceError(f"AI service failed: {e}") from e

    if response is None and last_error is not None:
        raise AIServiceError(f"AI service failed after retries: {last_error}") from last_error

    raw = getattr(response, "text", None) or ""
    if not str(raw).strip():
        raise AIServiceError("Empty response from AI model.")

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        raise AIServiceError(f"Invalid JSON from AI:\n{raw[:300]}") from e

    return parsed