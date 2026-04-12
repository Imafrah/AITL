from db.database import SessionLocal, Document

class DBError(Exception):
    pass

def save_document(document_id: str, source_file: str, document_type: str,
                  status: str, raw_text: str, structured_output: dict):
    db = SessionLocal()
    try:
        doc = Document(
            document_id=document_id,
            source_file=source_file,
            document_type=document_type,
            status=status,
            raw_text=raw_text,
            structured_output=structured_output
        )
        db.add(doc)
        db.commit()
    except Exception as e:
        db.rollback()
        raise DBError(f"Failed to save document: {e}") from e
    finally:
        db.close()

def get_document(document_id: str) -> dict | None:
    db = SessionLocal()
    try:
        doc = db.query(Document).filter(
            Document.document_id == document_id
        ).first()
        if not doc:
            return None
        return {
            "document_id": doc.document_id,
            "source_file": doc.source_file,
            "document_type": doc.document_type,
            "status": doc.status,
            "structured_output": doc.structured_output,
            "created_at": doc.created_at.isoformat()
        }
    except Exception as e:
        raise DBError(f"Failed to fetch document: {e}") from e
    finally:
        db.close()