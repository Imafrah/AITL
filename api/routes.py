import os
import asyncio
from pathlib import Path

from fastapi import APIRouter, UploadFile, File, HTTPException, Body
from fastapi.responses import PlainTextResponse

from db.crud import get_document, DBError
from orchestrator import run_pipeline
from post_processor.processor import convert_to_toml

router = APIRouter()

ALLOWED_EXTENSIONS = {"pdf", "csv", "txt"}
MAX_FILE_SIZE = 10 * 1024 * 1024


def _extension_from_filename(name: str | None) -> str | None:
    if not name or not name.strip():
        return None
    suffix = Path(name.strip()).suffix.lower().lstrip(".")
    return suffix or None


@router.post("/translate")
async def translate(file: UploadFile = File(...)):
    ext = _extension_from_filename(file.filename)
    if not ext or ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Unsupported or missing file extension: {ext or 'none'}. "
                "Allowed: pdf, csv, txt"
            ),
        )

    content = b""
    while True:
        chunk = await file.read(1024 * 1024)
        if not chunk:
            break
        content += chunk
        if len(content) > MAX_FILE_SIZE:
            raise HTTPException(status_code=422,
                detail="File too large. Maximum size is 10MB.")

    if len(content) == 0:
        raise HTTPException(status_code=422,
            detail="File is empty.")

    if not os.getenv("GEMINI_API_KEY"):
        raise HTTPException(status_code=500,
            detail="Server configuration error: GEMINI_API_KEY is not set.")

    try:
        if ext == "csv":
            from parsers.csv_parser import parse_csv_documents

            result = await asyncio.to_thread(parse_csv_documents, content)
        else:
            result = await asyncio.to_thread(run_pipeline, content, ext, file.filename)
    except Exception as e:
        raise HTTPException(status_code=500,
            detail=f"Processing failed: {str(e)}")

    return result


@router.post("/export/toml", response_class=PlainTextResponse)
def export_toml(payload: dict = Body(...)):
    """Convert a structured result dict to TOML without reading the database."""
    toml_output = convert_to_toml(payload)
    doc_id = payload.get("document_id") or "export"
    safe_name = str(doc_id).replace('"', "").replace("\n", "")[:200]
    return PlainTextResponse(
        content=toml_output,
        media_type="application/toml",
        headers={"Content-Disposition": f'attachment; filename="{safe_name}.toml"'},
    )


@router.get("/results/{document_id}")
def get_result(document_id: str):
    try:
        doc = get_document(document_id)
    except DBError as e:
        raise HTTPException(status_code=500, detail=str(e))

    if not doc:
        raise HTTPException(status_code=404,
            detail="Document not found.")

    return doc


@router.get("/results/{document_id}/toml", response_class=PlainTextResponse)
def get_result_toml(document_id: str):
    try:
        doc = get_document(document_id)
    except DBError as e:
        raise HTTPException(status_code=500, detail=str(e))

    if not doc:
        raise HTTPException(status_code=404, detail="Document not found.")

    toml_output = convert_to_toml(doc["structured_output"])
    return PlainTextResponse(
        content=toml_output,
        media_type="application/toml",
        headers={"Content-Disposition": f'attachment; filename="{document_id}.toml"'}
    )