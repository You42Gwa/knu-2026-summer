from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timezone

from langchain_ollama import OllamaEmbeddings

from database import get_chroma_collection
from core.config import OLLAMA_BASE_URL, EMBED_MODEL
CHROMA_BATCH    = 100
MIN_CHUNK_LEN   = 20

logger = logging.getLogger("ingest")

_collection      = None
_embeddings      = None
_collection_lock = threading.Lock()


def _get_collection():
    global _collection
    with _collection_lock:
        if _collection is None:
            _collection = get_chroma_collection(os.getenv("COLLECTION_NAME", "scholarship_rules"))
    return _collection


def _get_embeddings() -> OllamaEmbeddings:
    global _embeddings
    with _collection_lock:
        if _embeddings is None:
            _embeddings = OllamaEmbeddings(base_url=OLLAMA_BASE_URL, model=EMBED_MODEL)
    return _embeddings


def get_uploaded_at(file_path: str) -> str:
    ts = os.path.getmtime(file_path)
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def delete_from_chroma(source: str) -> int:
    """source 파일의 ChromaDB 문서를 삭제한다. 삭제된 수를 반환한다."""
    collection = _get_collection()
    try:
        existing = collection.get(where={"source": source}, include=[])
        count = len(existing["ids"])
        if count:
            collection.delete(where={"source": source})
        return count
    except Exception:
        return 0


def save_to_chroma(
    file_path: str,
    chunk_records: list[dict],
    file_hash: str,
    category: str,
) -> int:
    collection  = _get_collection()
    doc_name    = os.path.basename(file_path)
    abs_path    = os.path.abspath(file_path)
    ext         = os.path.splitext(file_path)[1].lower().lstrip(".")
    uploaded_at = get_uploaded_at(file_path)
    ingested_at = datetime.now(timezone.utc).isoformat()

    try:
        collection.delete(where={"source": doc_name})
    except Exception:
        pass

    documents, metadatas, ids = [], [], []
    doc_label = os.path.splitext(doc_name)[0]

    for idx, item in enumerate(chunk_records):
        text_val = item["text"].strip()
        if len(text_val) < MIN_CHUNK_LEN:
            continue
        text_val = f"[문서: {doc_label}]\n{text_val}"

        meta = {
            "source":      doc_name,
            "source_path": abs_path,
            "file_type":   ext,
            "category":    category,
            "file_hash":   file_hash,
            "chunk_index": idx,
            "uploaded_at": uploaded_at,
            "ingested_at": ingested_at,
        }
        if item.get("page") is not None:
            meta["page"] = item["page"]

        documents.append(text_val)
        metadatas.append(meta)
        ids.append(f"{doc_name}::chunk::{idx}")

    if not documents:
        logger.info("Chroma 저장 대상 없음 | file=%s", doc_name)
        return 0

    for i in range(0, len(documents), CHROMA_BATCH):
        batch_docs = documents[i : i + CHROMA_BATCH]
        batch_embeddings = _get_embeddings().embed_documents(batch_docs)
        collection.upsert(
            documents=batch_docs,
            embeddings=batch_embeddings,
            metadatas=metadatas[i : i + CHROMA_BATCH],
            ids=ids[i : i + CHROMA_BATCH],
        )

    logger.info("ChromaDB 저장 완료 | file=%s chunks=%d", doc_name, len(documents))
    return len(documents)
