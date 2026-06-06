"""
문서 수집 및 전처리 진입점.

각 파일 유형별 파서는 utils/parsers/ 하위 모듈에 구현되어 있습니다.
- PDF: utils/parsers/pdf_parser.py
- XLSX: utils/parsers/xlsx_parser.py
- HWP/HWPX: utils/parsers/hwp_parser.py

공통 유틸리티:
- utils/manifest.py       — PostgreSQL manifest CRUD
- utils/parquet_store.py  — Parquet 저장/삭제 (DATAFRAME_DIR 정의 포함)
- utils/chroma_store.py   — ChromaDB 저장
- utils/text_utils.py     — 텍스트 청킹, 개요 생성
- utils/table_parser.py   — 표 파싱 및 정제
"""

from __future__ import annotations

import glob
import hashlib
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging.handlers import RotatingFileHandler

# ---------------------------------------------------------------------------
# 로깅
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR  = os.path.join(BASE_DIR, "..", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("ingest")
logger.setLevel(logging.INFO)

if not logger.handlers:
    fmt = logging.Formatter("[%(asctime)s] %(levelname)s %(name)s - %(message)s")

    fh = RotatingFileHandler(
        os.path.join(LOG_DIR, "ingest.log"),
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    fh.setFormatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)

# ---------------------------------------------------------------------------
# 상수 (하위 모듈에서 재사용)
# ---------------------------------------------------------------------------
INGEST_WORKERS = 2

# DATAFRAME_DIR은 utils/parquet_store.py 에 정의돼 있습니다.
# 하위 호환성을 위해 여기서도 노출합니다.
from utils.parquet_store import DATAFRAME_DIR  # noqa: E402

from utils.manifest import (  # noqa: E402
    ensure_manifest_table,
    get_existing_file_hash,
    upsert_manifest,
)
from utils.parsers.xlsx_parser import ingest_xlsx  # noqa: E402
from utils.parsers.pdf_parser import ingest_pdf_hybrid  # noqa: E402
from utils.parsers.hwp_parser import convert_hwp_to_html_and_ingest  # noqa: E402


def compute_file_md5(file_path: str, chunk_size: int = 8192) -> str:
    md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        while chunk := f.read(chunk_size):
            md5.update(chunk)
    return md5.hexdigest()


def infer_category(file_path: str) -> str:
    parent = os.path.basename(os.path.dirname(file_path))
    return "uncategorized" if parent.lower() == "data" else parent


# ---------------------------------------------------------------------------
# 단일 파일 처리 진입점
# ---------------------------------------------------------------------------
def process_file(file_path: str):
    source      = os.path.basename(file_path)
    source_path = os.path.abspath(file_path)
    ext         = os.path.splitext(file_path)[1].lower().lstrip(".")
    category    = infer_category(file_path)
    file_hash   = compute_file_md5(file_path)

    if get_existing_file_hash(source) == file_hash:
        logger.info("생략(변경 없음) | file=%s", file_path)
        return

    logger.info("시작 | file=%s type=%s category=%s", file_path, ext, category)
    upsert_manifest(source, source_path, file_hash, ext, category, "IN_PROGRESS")

    try:
        chroma_doc_count = 0

        if ext == "xlsx":
            chroma_doc_count = ingest_xlsx(file_path, file_hash, category)
        elif ext == "pdf":
            chroma_doc_count = ingest_pdf_hybrid(file_path, file_hash, category)
        elif ext in ("hwp", "hwpx"):
            chroma_doc_count = convert_hwp_to_html_and_ingest(file_path, file_hash, category)
        else:
            logger.warning("지원하지 않는 확장자 | file=%s", file_path)
            return

        upsert_manifest(source, source_path, file_hash, ext, category, "SUCCESS",
                        chroma_doc_count=chroma_doc_count)
        logger.info("완료 | file=%s", file_path)

    except Exception as e:
        upsert_manifest(source, source_path, file_hash, ext, category, "FAILED",
                        error_message=str(e))
        logger.exception("실패 | file=%s", file_path)


# ---------------------------------------------------------------------------
# 직접 실행 시: data/ 폴더 병렬 처리
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    from dotenv import load_dotenv
    load_dotenv(os.path.join(BASE_DIR, "..", "..", ".env"))

    ensure_manifest_table()

    data_folder = os.path.join(BASE_DIR, "..", "data")
    if not os.path.exists(data_folder):
        print(f"'{data_folder}' 폴더가 없습니다.")
        sys.exit(1)

    file_paths = []
    for ext in ("xlsx", "pdf", "hwp", "hwpx"):
        file_paths.extend(
            glob.glob(os.path.join(data_folder, "**", f"*.{ext}"), recursive=True)
        )
    file_paths = [f for f in file_paths if not os.path.basename(f).startswith(".")]

    if not file_paths:
        print("처리할 파일이 없습니다.")
        sys.exit(0)

    print(f"총 {len(file_paths)}개 파일 병렬 처리 시작 (workers={INGEST_WORKERS})")

    with ThreadPoolExecutor(max_workers=INGEST_WORKERS) as executor:
        futures = {executor.submit(process_file, fp): fp for fp in file_paths}
        for future in as_completed(futures):
            fp = futures[future]
            try:
                future.result()
            except Exception:
                logger.exception("처리 실패 | file=%s", fp)

    print("\n모든 파일 처리 완료!")
