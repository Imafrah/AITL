import json
from google import genai
from google.genai import types

class AIServiceError(Exception):
    pass


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
            # Close any open brackets
            open_brackets = cleaned.count("{") - cleaned.count("}")
            cleaned += "}" * open_brackets
    
    return cleaned

def extract_entities(text: str, api_key: str, document_type: str = "unknown") -> dict:
    try:
        client = genai.Client(api_key=api_key)

        prompt = f"""
You are an information extraction system.

Extract entities from the following {document_type} document.

Return ONLY valid JSON.
Do NOT include explanations, markdown, or extra text.

Confidence scoring rules:
- 1.0 = explicitly stated in the document
- 0.85-0.95 = clearly implied but not exact
- 0.70-0.84 = inferred from context
- below 0.70 = uncertain or ambiguous

IMPORTANT:
- Dates must be in YYYY-MM-DD format
- Amount must be numeric
- If unsure, lower the confidence score

Format:
{{
  "document_type": "{document_type}",
  "entities": {{
    "person_names": [{{"value": "", "confidence": 0.0}}],
    "organizations": [{{"value": "", "confidence": 0.0}}],
    "dates": [{{"value": "", "confidence": 0.0}}],
    "amounts": [{{"value": 0, "currency": "", "label": "", "confidence": 0.0}}]
  }},
  "relationships": [
    {{
      "type": "payment",
      "from": "person name",
      "to": "organization name",
      "confidence": 0.0
    }}
  ]
}}

Document:
{text}
"""

        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config= {"temperature":0.2}
        )

        raw = response.text
        cleaned = clean_llm_response(raw)

        # 🔥 Safe JSON parsing
        try:
            parsed = json.loads(cleaned)
        except json.JSONDecodeError:
            raise AIServiceError(f"Invalid JSON from AI:\n{cleaned}")

        return parsed

    except Exception as e:
        raise AIServiceError(f"AI service failed: {e}")