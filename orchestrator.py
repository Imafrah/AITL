import os
import uuid
from parsers.router import route_file
from parsers.txt_parser import ParseError
from utils.data_cleaner import get_text_quality_score
from ai_layer.extractor import extract_entities, AIServiceError
from post_processor.processor import post_process, ValidationError
from db.crud import save_document, DBError
from logger import get_logger

logger = get_logger("orchestrator")

# Full file is always parsed + cleaned; this limit is only how much text is sent to the model.
MAX_AI_CHARS = int(os.getenv("AITL_MAX_AI_CHARS", "32000"))


def sample_csv_text(text: str, max_chars: int = 10000) -> str:
    """
    Prefer sending the entire cleaned CSV to the AI if it fits under max_chars.
    Otherwise take header + samples from beginning, middle, and end.
    """
    text = text.strip()
    if len(text) <= max_chars:
        return text

    lines = text.split("\n")
    if len(lines) <= 50:
        return text[:max_chars]

    header = lines[0]
    total = len(lines)

    sampled = (
        [header]
        + lines[1:20]
        + lines[total // 2 : total // 2 + 10]
        + lines[-10:]
    )
    return "\n".join(sampled)[:max_chars]


def detect_document_type(text: str, filename: str) -> str:
    """
    Detect document type from content PATTERNS — no hardcoded keyword lists.

    Uses statistical analysis of the text to classify:
    - Presence of structured numeric patterns (amounts, dates)
    - Text density and structure
    - Pattern-based detection
    """
    import re

    text_sample = text[:5000].lower() if text else ""
    filename_lower = filename.lower() if filename else ""

    if not text_sample.strip():
        return "unknown"

    # Count pattern occurrences (value-based, not keyword-based)
    amount_pattern = re.compile(r'[$€£₹¥]\s*[\d,]+\.?\d*|\b\d{1,3}(?:,\d{3})+\.\d{2}\b')
    date_pattern = re.compile(r'\b\d{4}-\d{2}-\d{2}\b|\b\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}\b')
    email_pattern = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')

    amount_count = len(amount_pattern.findall(text_sample))
    date_count = len(date_pattern.findall(text_sample))
    email_count = len(email_pattern.findall(text_sample))

    # Line structure analysis
    lines = text_sample.split('\n')
    non_empty_lines = [l for l in lines if l.strip()]
    total_lines = len(non_empty_lines)

    # Check for tabular structure (comma-separated or tab-separated)
    comma_lines = sum(1 for l in non_empty_lines if l.count(',') >= 2)
    tab_lines = sum(1 for l in non_empty_lines if l.count('\t') >= 2)
    is_tabular = (comma_lines / max(total_lines, 1) > 0.5 or
                  tab_lines / max(total_lines, 1) > 0.5)

    # Infer type from patterns
    if is_tabular:
        return "tabular_data"

    # High density of monetary values → financial document
    if amount_count >= 3:
        return "financial_document"

    # Mix of dates and structured text → structured record
    if date_count >= 2 and total_lines >= 5:
        return "structured_document"

    # Long-form text with few patterns → narrative/memo
    avg_line_len = sum(len(l) for l in non_empty_lines) / max(total_lines, 1)
    if avg_line_len > 80 and amount_count <= 1:
        return "narrative_document"

    return "generic_document"


def run_pipeline(file_bytes: bytes, file_type: str, filename: str) -> dict:
    api_key = os.getenv("GEMINI_API_KEY")
    logger.info(f"Pipeline started | file={filename} | type={file_type}")

    # Step 1: Parse
    try:
        parsed = route_file(file_bytes, file_type)
        logger.info(f"Parsing complete | words={parsed['metadata']['word_count']}")
    except ParseError as e:
        logger.error(f"Parse failed | {e}")
        return {
            "status": "failed",
            "error": f"Parse error: {str(e)}",
            "entities": {},
            "relationships": [],
            "metadata": {"file_type": file_type}
        }

    # Step 2: Detect document type from full text + filename (pattern-based)
    document_type = detect_document_type(parsed["text"], filename)
    logger.info(f"Document type detected: {document_type}")

    quality_score = get_text_quality_score(parsed["text"])
    logger.info(f"Text quality score: {quality_score}")
    parsed["metadata"]["text_quality_score"] = quality_score

    # Step 3: Truncate for AI only AFTER detection
    if file_type == "csv":
        text_for_ai = sample_csv_text(parsed["text"], MAX_AI_CHARS)
    else:
        text_for_ai = parsed["text"][:MAX_AI_CHARS]

    if len(parsed["text"]) > MAX_AI_CHARS:
        logger.warning(
            f"Text truncated to {MAX_AI_CHARS} chars | "
            f"original={len(parsed['text'])} chars"
        )

    # Step 4: AI Extraction
    try:
        ai_output = extract_entities(
            text_for_ai,
            api_key=api_key,
            document_type=document_type
        )
        logger.info("AI extraction complete")
    except AIServiceError as e:
        logger.error(f"AI extraction failed | {e}")
        return {
            "status": "partial",
            "error": f"AI extraction failed: {str(e)}",
            "entities": {},
            "relationships": [],
            "metadata": parsed["metadata"]
        }

    # Step 5: Post-Process
    try:
        result = post_process(
            ai_output,
            source_file=filename,
            file_metadata=parsed["metadata"]
        )
        logger.info(f"Post-processing complete | status={result['status']}")
    except ValidationError as e:
        logger.error(f"Post-processing failed | {e}")
        result = {
            "document_id": str(uuid.uuid4()),
            "document_type": ai_output.get("document_type", "unknown"),
            "source_file": filename,
            "status": "partial",
            "error": f"Post-processing failed: {str(e)}",
            "entities": ai_output.get("entities", {}),
            "relationships": ai_output.get("relationships", []),
            "metadata": parsed["metadata"],
        }

    # Step 6: Save to DB
    try:
        save_document(
            document_id=result["document_id"],
            source_file=filename,
            document_type=result.get("document_type", "unknown"),
            status=result["status"],
            raw_text=parsed["text"],
            structured_output=result
        )
        logger.info(f"Saved to DB | document_id={result['document_id']}")
    except DBError as e:
        logger.error(f"DB save failed | {e}")
        result["error"] = f"DB save failed: {str(e)}"
        result["status"] = "partial"

    logger.info(f"Pipeline complete | status={result['status']}")
    return result