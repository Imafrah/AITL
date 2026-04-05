import google.generativeai as genai
import json

genai.configure(api_key="AIzaSyCCVHY1RVd1wZkky5D-R7akW80E95GBIyQ")
model = genai.GenerativeModel("gemini-2.5-flash")

invoice_text = """
INVOICE #1001
Date: 12 January 2024
From: John Doe
To: ABC Corp
Amount: INR 5,000
Description: Web development services
"""

prompt = f"""
Extract entities from the following document.
Return ONLY valid JSON, no explanation, no markdown, no code blocks.

Format:
{{
  "document_type": "invoice",
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
{invoice_text}
"""

response = model.generate_content(
    prompt,
    generation_config={"response_mime_type": "application/json"}
)
raw = response.text
print("=== RAW OUTPUT ===")
print(raw)

print("\n=== JSON VALID? ===")
try:
    parsed = json.loads(raw)
    print("YES - valid JSON")
    print(f"Person found: {parsed['entities']['person_names']}")
    print(f"Org found:    {parsed['entities']['organizations']}")
    print(f"Amount found: {parsed['entities']['amounts']}")
except json.JSONDecodeError as e:
    print(f"NO - invalid JSON: {e}")