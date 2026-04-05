from fastapi import APIRouter, UploadFile, File, HTTPException
from db.crud import get_document, DBError
from orchestrator import run_pipeline

router = APIRouter()

ALLOWED_EXTENSIONS = {"pdf", "csv", "txt"}
MAX_FILE_SIZE = 10 * 1024 * 1024


@router.post("/translate")
async def translate(file: UploadFile = File(...)):
    ext = file.filename.split(".")[-1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=422,
            detail=f"Unsupported file type: {ext}. Allowed: pdf, csv, txt")

    content = await file.read()

    if len(content) > MAX_FILE_SIZE:
        raise HTTPException(status_code=422,
            detail="File too large. Maximum size is 10MB.")
    if len(content) == 0:
        raise HTTPException(status_code=422,
            detail="File is empty.")

    result = run_pipeline(content, ext, file.filename)
    return result


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