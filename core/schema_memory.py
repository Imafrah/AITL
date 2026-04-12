import hashlib
import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_DB = os.path.join(os.path.dirname(os.path.dirname(__file__)), "schema_memory.db")


def _db_path() -> str:
    return os.getenv("SCHEMA_MEMORY_DB", _DEFAULT_DB)


def _connect() -> sqlite3.Connection:
    path = _db_path()
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.isdir(parent):
        os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False, timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn


def init_schema_memory() -> None:
    """Create schema_memory table if missing (O(1) lookup by signature)."""
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_memory (
                signature TEXT PRIMARY KEY,
                mapping TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.commit()
    logger.info("Schema memory ready at %s", _db_path())


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
        with _connect() as conn:
            row = conn.execute(
                "SELECT mapping FROM schema_memory WHERE signature = ?",
                (sig,),
            ).fetchone()
        if not row:
            return None
        return json.loads(row["mapping"])
    except Exception as e:
        logger.warning("Schema memory read failed: %s", e)
        return None


def save_schema_to_memory(columns: list[str], mapping: dict[str, Any]) -> None:
    """Upsert schema payload for this column signature."""
    sig = generate_signature(columns)
    now = datetime.now(timezone.utc).isoformat()
    body = json.dumps(mapping, default=str)
    try:
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO schema_memory (signature, mapping, created_at)
                VALUES (?, ?, ?)
                ON CONFLICT(signature) DO UPDATE SET
                    mapping = excluded.mapping,
                    created_at = excluded.created_at
                """,
                (sig, body, now),
            )
            conn.commit()
        logger.info("Schema saved to memory | signature=%s", sig[:12])
    except Exception as e:
        logger.warning("Schema memory write failed: %s", e)
