from __future__ import annotations

from datetime import datetime

from sqlalchemy import text

from database import engine


def ensure_manifest_table():
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ingestion_manifest (
                source           TEXT PRIMARY KEY,
                source_path      TEXT,
                file_hash        TEXT NOT NULL,
                file_type        TEXT,
                category         TEXT,
                processed_at     TIMESTAMP NOT NULL,
                status           TEXT NOT NULL,
                error_message    TEXT,
                chroma_doc_count INTEGER DEFAULT 0
            )
        """))


def get_existing_file_hash(source: str) -> str | None:
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT file_hash FROM ingestion_manifest WHERE source = :s"),
            {"s": source},
        ).fetchone()
    return row[0] if row else None


def upsert_manifest(
    source: str,
    source_path: str,
    file_hash: str,
    file_type: str,
    category: str,
    status: str,
    error_message: str | None = None,
    chroma_doc_count: int = 0,
):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO ingestion_manifest
                (source, source_path, file_hash, file_type, category,
                 processed_at, status, error_message, chroma_doc_count)
            VALUES
                (:source, :source_path, :file_hash, :file_type, :category,
                 :processed_at, :status, :error_message, :chroma_doc_count)
            ON CONFLICT (source) DO UPDATE SET
                source_path      = EXCLUDED.source_path,
                file_hash        = EXCLUDED.file_hash,
                file_type        = EXCLUDED.file_type,
                category         = EXCLUDED.category,
                processed_at     = EXCLUDED.processed_at,
                status           = EXCLUDED.status,
                error_message    = EXCLUDED.error_message,
                chroma_doc_count = EXCLUDED.chroma_doc_count
        """), {
            "source": source, "source_path": source_path,
            "file_hash": file_hash, "file_type": file_type,
            "category": category, "processed_at": datetime.now(),
            "status": status, "error_message": error_message,
            "chroma_doc_count": chroma_doc_count,
        })
