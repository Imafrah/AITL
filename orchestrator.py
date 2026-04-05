import os
from parsers.router import route_file
from parsers.txt_parser import ParseError
from ai_layer.extractor import extract_entities, AIServiceError
from post_processor.processor import post_process, ValidationError
from db.crud import save_document, DBError
from logger import get_logger
logger = get_logger("orchestrator")

MAX_AI_CHARS = 3000  # Prevent token overflow for large files

def detect_document_type(text: str, filename: str) -> str:
    text_lower = text.lower()
    filename_lower = filename.lower()

    if any(k in text_lower or k in filename_lower
           for k in ["invoice", "inv-", "bill", "payment due", "amount due"]):
        return "invoice"

    if any(k in text_lower or k in filename_lower
           for k in ["transaction", "debit", "credit", "balance", "account"]):
        return "financial_report"

    if any(k in text_lower or k in filename_lower
           for k in ["agreement", "contract", "terms", "party", "clause"]):
        return "contract"

    if any(k in text_lower or k in filename_lower
           for k in ["memo", "subject:", "re:"]):
        return "memo"

    return "unknown"

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

    # Step 2: AI Extraction
    try:
        # Truncate text to avoid token overflow on large files
        text_for_ai = parsed["text"][:MAX_AI_CHARS]
        was_truncated = len(parsed["text"]) > MAX_AI_CHARS
        if was_truncated:
            logger.warning(f"Text truncated to {MAX_AI_CHARS} chars for AI | original={len(parsed['text'])}")

        # Auto-detect document type from filename
        document_type = detect_document_type(text_for_ai, filename)
        logger.info(f"Detected document type: {document_type}")

        detected_type = detect_document_type(parsed["text"], filename)
        logger.info(f"Document type detected: {detected_type}")
        ai_output = extract_entities(
            parsed["text"], api_key=api_key, document_type=detected_type
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

    # Step 3: Post-Process
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
            "status": "partial",
            "error": f"Post-processing failed: {str(e)}",
            "entities": ai_output.get("entities", {}),
            "relationships": [],
            "metadata": parsed["metadata"]
        }

    # Step 4: Save to DB
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