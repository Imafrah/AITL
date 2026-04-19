import json
import logging
import time
from typing import Any

from google import genai
from google.genai import types

logger = logging.getLogger(__name__)

PROMPT_TEMPLATE = """You are a PRODUCTION-GRADE GENERIC DATA CLEANING ENGINE.

Your task is to clean ANY structured dataset (JSON array) without prior knowledge of its schema.

========================
🚨 CORE PRINCIPLES
==================
1. ❌ NO HARDCODING
   * Do NOT assume column names (e.g., year, amount, etc.)
   * Dynamically detect schema from data
   * Work for ANY dataset structure

2. ❌ NO DATA FABRICATION
   * Never create fake values
   * If data is missing → use NULL

3. ❌ NO DATA CORRUPTION
   * Never overwrite valid values
   * Never copy values across rows

4. ✅ PRESERVE SEMANTIC MEANING
   * Clean data WITHOUT changing meaning

========================
🔍 AUTO-DETECTION LOGIC
=======================
For EACH column, automatically detect type:
* If values are mostly numbers → NUMERIC
* If values match date patterns → DATE
* If values are text → STRING
* If mixed → treat carefully (convert safely or keep as STRING)

========================
🧹 CLEANING RULES (GENERIC)
===========================
1. TEXT CLEANING
   * Remove noise: special symbols (*, †, ‡, [], (), footnotes)
   * Trim spaces
   * Normalize casing (Title Case for names, lower/upper where appropriate)
   * Remove duplicate whitespace
2. NUMERIC CLEANING
   * Remove commas, currency symbols, text noise
   * Convert to proper numeric type
   * Invalid numeric → NULL
3. DATE CLEANING
   * Parse into standard format (YYYY-MM-DD if possible)
   * If incomplete or invalid → NULL
4. MISSING VALUES
   * Empty / NaN / invalid → NULL
   * Do NOT guess or interpolate unless explicitly safe
5. INCONSISTENCIES
   * Standardize similar values (e.g., "Unknown", "N/A", "" → NULL)
   * Ensure consistent formats within each column
6. DUPLICATES
   * Remove exact duplicate rows
   * Keep unique records only
7. ANOMALY HANDLING
   * Detect extreme or inconsistent values
   * DO NOT modify them
   * Keep original values (truth over assumption)
8. TYPE CONSISTENCY
   * Ensure each column has a single consistent type
   * Avoid mixed types unless unavoidable

========================
✅ VALIDATION RULES
==================
* No corrupted values
* No fake/generated values
* Schema consistent across all rows
* Data remains truthful and usable

========================
📤 OUTPUT RULE (STRICT)
=======================
Return ONLY the cleaned dataset as a continuous JSON array of objects.
❌ No explanations
❌ No markdown formatting besides the raw JSON string
❌ No metadata
❌ No comments
❌ No assumptions

========================
TARGET JSON RECORDS:
{records_json}
"""

def ai_clean_dataset(records: list[dict[str, Any]], api_key: str) -> list[dict[str, Any]]:
    if not records:
        return []
        
    if not api_key:
        logger.warning("No API key provided for ai_clean_dataset. Returning original records.")
        return records

    try:
        client = genai.Client(api_key=api_key)
    except Exception as e:
        logger.warning("Could not initialize genai Client: %s", e)
        return records
    
    # Chunking: 50 records per API call to avoid blowing up context/response token limits
    CHUNK_SIZE = 50
    all_cleaned = []
    
    for i in range(0, len(records), CHUNK_SIZE):
        chunk = records[i:i + CHUNK_SIZE]
        records_json = json.dumps(chunk, default=str)
        prompt = PROMPT_TEMPLATE.replace("{records_json}", records_json)
        
        try:
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.0,
                    response_mime_type="application/json",
                ),
            )
            raw = getattr(response, "text", None) or ""
            if not raw.strip():
                all_cleaned.extend(chunk)
                continue
                
            cleaned_chunk = json.loads(raw)
            if isinstance(cleaned_chunk, list):
                all_cleaned.extend(cleaned_chunk)
            elif isinstance(cleaned_chunk, dict) and "data" in cleaned_chunk:
                all_cleaned.extend(cleaned_chunk["data"])
            else:
                logger.warning("AI cleaner returned non-list JSON, falling back.")
                all_cleaned.extend(chunk)
                
        except Exception as e:
            logger.error("AI Cleaner failed for a chunk: %s", e)
            all_cleaned.extend(chunk)  # Fallback to original
            time.sleep(1) # Prevent aggressive retries causing rate limits if crashing
            
    return all_cleaned
