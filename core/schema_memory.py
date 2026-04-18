import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any

from db.database import SessionLocal, SchemaMemory

logger = logging.getLogger(__name__)


def init_schema_memory() -> None:
    """Initialization is handled by db.database.init_db(). Preserved for app startup compatibility."""
    logger.info("Schema memory correctly wired to SQLAlchemy persistent storage.")


def generate_signature(columns: list[str]) -> str:
    """MD5 of sorted normalized lowercase column names (stable for same schema)."""
    from parsers.csv_parser import normalize_field_name

    norm = sorted(
        {normalize_field_name(str(c)) for c in columns if c is not None and str(c).strip()}
    )
    payload = "|".join(norm).encode("utf-8")
    return hashlib.md5(payload).hexdigest()


def get_schema_from_memory(columns: list[str]) -> dict[str, Any] | None:
    """Return cached schema payload if signature exists, else None."""
    sig = generate_signature(columns)
    try:
        with SessionLocal() as session:
            record = session.query(SchemaMemory).filter_by(signature=sig).first()
            if not record:
                return None
            logger.info("⚡ Using cached schema | signature=%s", sig[:12])
            return json.loads(record.mapping)
    except Exception as e:
        logger.warning("Schema memory read failed: %s", e)
        return None


def save_schema_to_memory(columns: list[str], mapping: dict[str, Any]) -> None:
    """Upsert schema payload for this column signature."""
    sig = generate_signature(columns)
    now = datetime.now(timezone.utc)
    body = json.dumps(mapping, default=str)
    try:
        with SessionLocal() as session:
            record = session.query(SchemaMemory).filter_by(signature=sig).first()
            if record:
                record.mapping = body
                record.created_at = now
            else:
                record = SchemaMemory(
                    signature=sig,
                    mapping=body,
                    created_at=now
                )
                session.add(record)
            session.commit()
        logger.info("💾 Schema saved | signature=%s", sig[:12])
    except Exception as e:
        logger.warning("Schema memory write failed: %s", e)
