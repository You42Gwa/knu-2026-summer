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


def get_all_manifest_entries() -> list[dict]:
    """모든 색인 문서 목록을 최신순으로 반환한다."""
    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT source, status, file_type, chroma_doc_count, processed_at
                FROM ingestion_manifest
                ORDER BY processed_at DESC
            """)
        ).fetchall()
    return [
        {
            "source": r[0],
            "status": r[1],
            "file_type": r[2],
            "chroma_doc_count": r[3],
            "processed_at": str(r[4]) if r[4] else None,
        }
        for r in rows
    ]


def get_manifest_status(source: str) -> dict | None:
    """source(파일명) 의 색인 상태를 조회한다. 기록이 없으면 None."""
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT status, chroma_doc_count, error_message, processed_at, file_type
                FROM ingestion_manifest WHERE source = :s
            """),
            {"s": source},
        ).fetchone()
    if row is None:
        return None
    return {
        "source": source,
        "status": row[0],
        "chroma_doc_count": row[1],
        "error_message": row[2],
        "processed_at": str(row[3]) if row[3] else None,
        "file_type": row[4],
    }


def get_existing_file_hash(source: str) -> str | None:
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT file_hash FROM ingestion_manifest WHERE source = :s"),
            {"s": source},
        ).fetchone()
    return row[0] if row else None


def delete_manifest(source: str) -> bool:
    """source 항목을 manifest에서 삭제한다. 삭제된 행이 있으면 True."""
    with engine.begin() as conn:
        result = conn.execute(
            text("DELETE FROM ingestion_manifest WHERE source = :s"),
            {"s": source},
        )
    return result.rowcount > 0


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
