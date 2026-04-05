import json
from google import genai

class AIServiceError(Exception):
    pass


# ── Prompt Registry ───────────────────────────────────────────────────────────

PROMPTS = {
    "invoice": """
You are a precise invoice data extraction engine.
Return ONLY valid JSON. No explanation, no markdown, no code blocks.

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

OUTPUT FORMAT:
{{
  "document_type": "invoice",
  "entities": {{
    "person_names": [{{"value": "", "confidence": 0.0}}],
    "organizations": [{{"value": "", "confidence": 0.0}}],
    "dates": [{{"value": "", "label": "", "confidence": 0.0}}],
    "amounts": [{{"value": 0, "currency": "", "label": "", "confidence": 0.0}}]
  }},
  "relationships": [
    {{
      "type": "payment",
      "from": "buyer name",
      "to": "vendor name",
      "confidence": 0.0,
      "attributes": {{}}
    }}
  ]
}}

DOCUMENT:
{text}
""",

    "employee_record": """
You are a precise HR data extraction engine.
Return ONLY valid JSON. No explanation, no markdown, no code blocks.

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

OUTPUT FORMAT:
{{
  "document_type": "employee_record",
  "entities": {{
    "person_names": [{{"value": "", "confidence": 0.0}}],
    "organizations": [{{"value": "", "confidence": 0.0}}],
    "dates": [{{"value": "", "label": "date_of_birth", "confidence": 0.0}}],
    "amounts": [{{"value": 0, "currency": "", "label": "salary", "confidence": 0.0}}]
  }},
  "relationships": [
    {{
      "type": "employed_by",
      "from": "employee name",
      "to": "department name",
      "confidence": 0.0,
      "attributes": {{}}
    }}
  ]
}}

DOCUMENT:
{text}
""",

    "financial_report": """
You are a precise financial data extraction engine.
Return ONLY valid JSON. No explanation, no markdown, no code blocks.

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

OUTPUT FORMAT:
{{
  "document_type": "financial_report",
  "entities": {{
    "person_names": [{{"value": "", "confidence": 0.0}}],
    "organizations": [{{"value": "", "confidence": 0.0}}],
    "dates": [{{"value": "", "label": "", "confidence": 0.0}}],
    "amounts": [{{"value": 0, "currency": "", "label": "", "confidence": 0.0}}]
  }},
  "relationships": [
    {{
      "type": "transaction",
      "from": "sender",
      "to": "receiver",
      "confidence": 0.0,
      "attributes": {{}}
    }}
  ]
}}

DOCUMENT:
{text}
""",

    "contract": """
You are a precise legal document extraction engine.
Return ONLY valid JSON. No explanation, no markdown, no code blocks.

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

OUTPUT FORMAT:
{{
  "document_type": "contract",
  "entities": {{
    "person_names": [{{"value": "", "confidence": 0.0}}],
    "organizations": [{{"value": "", "confidence": 0.0}}],
    "dates": [{{"value": "", "label": "", "confidence": 0.0}}],
    "amounts": [{{"value": 0, "currency": "", "label": "", "confidence": 0.0}}]
  }},
  "relationships": [
    {{
      "type": "agreement",
      "from": "party one",
      "to": "party two",
      "confidence": 0.0,
      "attributes": {{}}
    }}
  ]
}}

DOCUMENT:
{text}
""",

    "unknown": """
You are a precise data extraction engine.
Return ONLY valid JSON. No explanation, no markdown, no code blocks.

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

OUTPUT FORMAT:
{{
  "document_type": "unknown",
  "entities": {{
    "person_names": [{{"value": "", "confidence": 0.0}}],
    "organizations": [{{"value": "", "confidence": 0.0}}],
    "dates": [{{"value": "", "label": "", "confidence": 0.0}}],
    "amounts": [{{"value": 0, "currency": "", "label": "", "confidence": 0.0}}]
  }},
  "relationships": []
}}

DOCUMENT:
{text}
"""
}


# ── LLM Response Cleaner ──────────────────────────────────────────────────────

def clean_llm_response(raw: str) -> str:
    cleaned = raw.strip()
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        cleaned = "\n".join(lines).strip()

    # If JSON is truncated, try to salvage it
    if not cleaned.endswith("}"):
        last_brace = cleaned.rfind("}")
        if last_brace != -1:
            cleaned = cleaned[:last_brace+1]
            open_brackets = cleaned.count("{") - cleaned.count("}")
            cleaned += "}" * open_brackets

    return cleaned


# ── Main Extractor ────────────────────────────────────────────────────────────

def extract_entities(text: str, api_key: str, document_type: str = "unknown") -> dict:
    try:
        client = genai.Client(api_key=api_key)

        # Select prompt from registry, fall back to unknown
        prompt_template = PROMPTS.get(document_type, PROMPTS["unknown"])
        prompt = prompt_template.format(text=text)

        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config={"temperature": 0.2}
        )

        raw = response.text
        cleaned = clean_llm_response(raw)

        try:
            parsed = json.loads(cleaned)
        except json.JSONDecodeError:
            raise AIServiceError(f"Invalid JSON from AI:\n{cleaned[:300]}")

        return parsed

    except Exception as e:
        raise AIServiceError(f"AI service failed: {e}")