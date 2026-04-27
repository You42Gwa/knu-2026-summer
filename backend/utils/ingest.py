"""
문서 수집 및 전처리 모듈.

- PDF (텍스트): 표 → PostgreSQL, 텍스트(표 제외) → ChromaDB
- PDF (스캔 이미지): OCR(pytesseract) → ChromaDB
- HWP: hwp5html 변환 후 표 → PostgreSQL, 텍스트 → ChromaDB
- XLSX: 시트별 → PostgreSQL
- MD5 해시 기반 중복 방지 / RecursiveCharacterTextSplitter 청킹
"""

import os
import sys
import re
import subprocess
import shutil
import hashlib
import logging
import threading
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import pdfplumber
from bs4 import BeautifulSoup
from sqlalchemy import text, inspect
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_ollama import OllamaEmbeddings

# OCR 선택적 임포트 (미설치 시 스캔 PDF 처리 불가 경고)
try:
    from pdf2image import convert_from_path
    import pytesseract
    HAS_OCR = True
except ImportError:
    HAS_OCR = False

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".env"))

from database import engine, get_chroma_collection

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

if not HAS_OCR:
    logger.warning("pdf2image/pytesseract 미설치 — 스캔 PDF는 OCR 없이 건너뜁니다.")

# ---------------------------------------------------------------------------
# 상수
# ---------------------------------------------------------------------------
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
EMBED_MODEL     = os.getenv("EMBED_MODEL", "qwen3-embedding:0.6b")

CHUNK_SIZE     = 500
CHUNK_OVERLAP  = 100
MIN_CHUNK_LEN  = 20
CHROMA_BATCH   = 100
INGEST_WORKERS = 2
OCR_DPI        = 300
OCR_LANG       = "kor+eng"

_splitter = RecursiveCharacterTextSplitter(
    chunk_size=CHUNK_SIZE,
    chunk_overlap=CHUNK_OVERLAP,
    separators=["\n\n", "\n", ". ", " ", ""],
)

# ChromaDB 컬렉션 / 임베딩 싱글턴 (스레드 안전)
_collection      = None
_embeddings      = None
_collection_lock = threading.Lock()

def _get_collection():
    global _collection
    with _collection_lock:
        if _collection is None:
            _collection = get_chroma_collection("scholarship_rules")
    return _collection

def _get_embeddings() -> OllamaEmbeddings:
    global _embeddings
    with _collection_lock:
        if _embeddings is None:
            _embeddings = OllamaEmbeddings(base_url=OLLAMA_BASE_URL, model=EMBED_MODEL)
    return _embeddings

# ---------------------------------------------------------------------------
# 유틸리티
# ---------------------------------------------------------------------------
def sanitize_table_name(name: str) -> str:
    original = name
    name = re.sub(r"[^\x00-\x7F]", "", name)   # 한글 등 non-ASCII 제거
    name = re.sub(r"[^a-zA-Z0-9]", "_", name)  # 특수문자 → _
    name = re.sub(r"_+", "_", name).strip("_") # 연속 _ 정리
    name = name.lower()[:32].rstrip("_")        # 소문자 + 32자 제한 (해시 공간 확보)
    if not name:
        # 한글 전용 파일명 등 ASCII 결과가 없으면 MD5 앞 8자리로 식별
        name = "tbl_" + hashlib.md5(original.encode("utf-8")).hexdigest()[:8]
    elif name[0].isdigit():
        name = "tbl_" + name
    return name


def sanitize_column_name(col: str) -> str:
    """컬럼명에서 특수문자를 제거해 SQL 쿼리 오류를 방지한다. 한글·영문·숫자는 유지."""
    col = str(col).strip()
    if not col or col in ("None", "nan"):
        return None
    col = re.sub(r"[^\w가-힣]", "_", col, flags=re.UNICODE)
    col = re.sub(r"_+", "_", col).strip("_")
    col = col[:40]  # 컬럼명 길이 제한
    if not col:
        return None
    if col[0].isdigit():
        col = "col_" + col
    return col


def _cell_val(cell) -> str:
    return str(cell).strip() if cell is not None else ""


def _parse_table(raw_table: list[list]) -> "pd.DataFrame | None":
    """병합 셀(None) 처리 + 2행 헤더 자동 탐지 후 DataFrame 반환."""
    if not raw_table or len(raw_table) < 2:
        return None

    ncols = max(len(r) for r in raw_table)
    # 모든 행을 ncols로 패딩
    table = [list(r) + [None] * (ncols - len(r)) for r in raw_table]

    # 유효 셀 비율 40% 이상인 첫 번째 행 → 헤더
    header_idx = 0
    for i, row in enumerate(table):
        if sum(1 for c in row if _cell_val(c)) >= ncols * 0.4:
            header_idx = i
            break

    h1 = table[header_idx]
    data_start = header_idx + 1

    # 다음 행이 서브헤더인지 확인
    # 기준: h1에서 빈 위치의 50% 이상을 다음 행이 채움
    if data_start < len(table):
        h2 = table[data_start]
        empty_pos = [j for j in range(ncols) if not _cell_val(h1[j])]
        fills = sum(1 for j in empty_pos if _cell_val(h2[j]))
        if empty_pos and fills >= len(empty_pos) * 0.5:
            # h1 + h2 병합: h1이 비어있는 위치는 h2 값 사용
            merged = [_cell_val(h2[j]) if not _cell_val(h1[j]) else _cell_val(h1[j])
                      for j in range(ncols)]
            data_start += 1
        else:
            merged = [_cell_val(c) for c in h1]
    else:
        merged = [_cell_val(c) for c in h1]

    # 컬럼명: None 위치는 이전 값으로 채운 뒤 sanitize + 중복 처리
    filled_headers: list[str] = []
    last = ""
    for v in merged:
        last = v if v else last
        filled_headers.append(last)

    seen: dict[str, int] = {}
    headers = []
    for j, h in enumerate(filled_headers):
        name = sanitize_column_name(h) or f"col_{j}"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        headers.append(name)

    # 데이터 행 처리: 수평 병합(None) 채우기
    def ffill_row(row):
        result, last = [], None
        for cell in row:
            v = _cell_val(cell)
            if v:
                last = v
            result.append(last)
        return result

    data_rows = [ffill_row(r) for r in table[data_start:]]

    df = pd.DataFrame(data_rows, columns=headers)
    df = df.replace("", None)
    df = df.ffill(axis=0)          # 수직 병합 셀 채움
    df = df.dropna(how="all").replace("\n", " ", regex=True)
    return df if not df.empty else None


def compute_file_md5(file_path: str, chunk_size: int = 8192) -> str:
    md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        while chunk := f.read(chunk_size):
            md5.update(chunk)
    return md5.hexdigest()


def infer_category(file_path: str) -> str:
    parent = os.path.basename(os.path.dirname(file_path))
    return "uncategorized" if parent.lower() == "data" else parent


def get_uploaded_at(file_path: str) -> str:
    ts = os.path.getmtime(file_path)
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def read_text_with_fallbacks(file_path: str, encodings=("utf-8", "cp949", "euc-kr")) -> str:
    with open(file_path, "rb") as f:
        raw = f.read()
    for enc in encodings:
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def clean_pdf_text(raw: str) -> str:
    """PDF 추출 텍스트 노이즈 제거."""
    # \w 대신 [^\s] 사용 → 한국어 포함 모든 비공백 문자에서 하이픈 연결
    raw = re.sub(r"([^\s])-\n([^\s])", r"\1\2", raw)
    raw = re.sub(r"\n{3,}", "\n\n", raw)
    # 단독 페이지 번호 줄 제거
    raw = re.sub(r"^\s*\d+\s*$", "", raw, flags=re.MULTILINE)
    return raw.strip()


def split_into_chunks(raw: str, page: int | None = None) -> list[dict]:
    return [
        {"text": c, "page": page}
        for c in _splitter.split_text(raw)
        if len(c.strip()) >= MIN_CHUNK_LEN
    ]

# ---------------------------------------------------------------------------
# manifest 관리
# ---------------------------------------------------------------------------
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


def _drop_table_and_type(conn, name: str):
    """테이블과 연관 composite type을 함께 삭제."""
    conn.execute(text(f'DROP TABLE IF EXISTS public."{name}" CASCADE'))
    conn.execute(text(f'DROP TYPE IF EXISTS public."{name}" CASCADE'))


def drop_tables_with_prefix(prefix: str):
    inspector = inspect(engine)
    targets = [t for t in inspector.get_table_names(schema="public") if t.startswith(prefix)]
    with engine.begin() as conn:
        for name in targets:
            _drop_table_and_type(conn, name)
    if targets:
        logger.info("기존 테이블 %d개 삭제 (prefix=%s)", len(targets), prefix)

# ---------------------------------------------------------------------------
# ChromaDB 저장
# ---------------------------------------------------------------------------
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
    # 파일명에서 확장자 제거해 문서 레이블로 사용
    doc_label = os.path.splitext(doc_name)[0]

    for idx, item in enumerate(chunk_records):
        text_val = item["text"].strip()
        if len(text_val) < MIN_CHUNK_LEN:
            continue
        # 청크 앞에 문서 출처 추가 → 검색 시 문서 맥락 포함
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

    # qwen3-embedding으로 임베딩 생성 후 명시적으로 전달 (ChromaDB 기본 임베딩과 혼용 방지)
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

# ---------------------------------------------------------------------------
# XLSX → PostgreSQL (다중 시트 지원)
# ---------------------------------------------------------------------------
def ingest_xlsx_to_postgres(file_path: str):
    logger.info("[XLSX] %s", file_path)
    base_name = sanitize_table_name(os.path.basename(file_path).rsplit(".", 1)[0])

    xl = pd.ExcelFile(file_path, engine="openpyxl")
    sheets = xl.sheet_names

    for sheet_name in sheets:
        # header=None으로 읽어 제목행을 _parse_table이 처리하게 함
        raw_df = xl.parse(sheet_name, header=None)
        if raw_df.empty:
            logger.info("빈 시트 건너뜀 | sheet=%s", sheet_name)
            continue

        raw_table = [
            [None if (v is None or (isinstance(v, float) and __import__('math').isnan(v))) else v
             for v in row]
            for row in raw_df.values.tolist()
        ]
        df = _parse_table(raw_table)
        if df is None:
            logger.warning("XLSX 파싱 결과 없음 | sheet=%s", sheet_name)
            continue

        table_name = (
            f"{base_name}_{sanitize_table_name(sheet_name)}"
            if len(sheets) > 1
            else base_name
        )
        with engine.begin() as conn:
            _drop_table_and_type(conn, table_name)
        df.to_sql(table_name, engine, if_exists="fail", index=False)
        logger.info("[XLSX] '%s' 적재 완료 | sheet=%s rows=%d", table_name, sheet_name, len(df))

# ---------------------------------------------------------------------------
# PDF 페이지별 텍스트 추출 (스캔 감지 + OCR 폴백)
# ---------------------------------------------------------------------------
def _extract_page_texts(file_path: str) -> dict[int, str]:
    """
    페이지 번호 → 텍스트 딕셔너리 반환.
    텍스트 레이어가 없는 페이지는 OCR로 추출한다.
    """
    page_texts: dict[int, str] = {}
    scanned_pages: list[int] = []

    with pdfplumber.open(file_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            try:
                # 표 영역 bbox 수집
                table_bboxes = [tbl.bbox for tbl in page.find_tables()]

                if table_bboxes:
                    # 표 영역의 객체를 제외하고 텍스트 추출 (중복 방지)
                    def not_in_table(obj):
                        for bbox in table_bboxes:
                            if (obj.get("x0", 0) >= bbox[0] - 1 and
                                    obj.get("x1", 0) <= bbox[2] + 1 and
                                    obj.get("top", 0) >= bbox[1] - 1 and
                                    obj.get("bottom", 0) <= bbox[3] + 1):
                                return False
                        return True

                    raw = page.filter(not_in_table).extract_text() or ""
                else:
                    raw = page.extract_text() or ""

            except Exception:
                logger.exception("pdfplumber 텍스트 추출 실패 | page=%d", page_num)
                raw = ""

            if raw.strip():
                page_texts[page_num] = raw
            else:
                scanned_pages.append(page_num)

    # OCR 처리 (스캔 페이지) — 페이지 단위로 변환해 메모리 절약
    if scanned_pages:
        if not HAS_OCR:
            logger.warning(
                "스캔 페이지 %s 감지됐으나 pytesseract/pdf2image 미설치로 건너뜀 | file=%s",
                scanned_pages, file_path,
            )
        else:
            logger.info("OCR 시작 | file=%s 스캔 페이지=%s", file_path, scanned_pages)
            for page_num in scanned_pages:
                try:
                    images = convert_from_path(
                        file_path, dpi=OCR_DPI,
                        first_page=page_num, last_page=page_num,
                    )
                    ocr_text = pytesseract.image_to_string(images[0], lang=OCR_LANG)
                    if ocr_text.strip():
                        page_texts[page_num] = ocr_text
                        logger.info("OCR 완료 | page=%d chars=%d", page_num, len(ocr_text))
                except Exception:
                    logger.exception("OCR 실패 | page=%d", page_num)

    return page_texts

# ---------------------------------------------------------------------------
# PDF → 표: PostgreSQL / 텍스트: ChromaDB
# ---------------------------------------------------------------------------
def ingest_pdf_hybrid(file_path: str, file_hash: str, category: str) -> int:
    logger.info("[PDF] %s", file_path)

    safe_name = sanitize_table_name(os.path.basename(file_path).rsplit(".", 1)[0])
    drop_tables_with_prefix(f"{safe_name}_p")

    # 페이지별 텍스트 추출 (OCR 포함)
    page_texts = _extract_page_texts(file_path)

    chunk_records: list[dict] = []
    for page_num, raw_text in page_texts.items():
        cleaned = clean_pdf_text(raw_text)
        chunk_records.extend(split_into_chunks(cleaned, page=page_num))

    # 표 추출 → PostgreSQL
    table_count = 0
    with pdfplumber.open(file_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            try:
                tables = page.extract_tables()
            except Exception:
                logger.exception("PDF 표 추출 실패 | page=%d", page_num)
                continue

            for t_idx, table in enumerate(tables):
                try:
                    df = _parse_table(table)
                    if df is None:
                        continue
                    tbl = f"{safe_name}_p{page_num}_t{table_count}"
                    with engine.begin() as conn:
                        _drop_table_and_type(conn, tbl)
                    df.to_sql(tbl, engine, if_exists="fail", index=False)
                    logger.info("[PDF] 표 저장 | tbl=%s rows=%d", tbl, len(df))
                    table_count += 1
                except Exception:
                    logger.exception("[PDF] 표 저장 실패 | page=%d t=%d", page_num, t_idx)

    if not chunk_records and table_count == 0:
        logger.warning("추출 데이터 없음 | file=%s", file_path)

    chroma_count = save_to_chroma(file_path, chunk_records, file_hash, category) if chunk_records else 0
    logger.info("PDF 완료 | file=%s tables=%d chunks=%d", file_path, table_count, chroma_count)
    return chroma_count

# ---------------------------------------------------------------------------
# HWP → HTML → 표: PostgreSQL / 텍스트: ChromaDB
# ---------------------------------------------------------------------------
def convert_hwp_to_html_and_ingest(file_path: str, file_hash: str, category: str) -> int:
    logger.info("[HWP] %s", file_path)

    safe_name = sanitize_table_name(os.path.basename(file_path).rsplit(".", 1)[0])
    drop_tables_with_prefix(f"{safe_name}_html_t")

    html_dir = os.path.join(os.path.dirname(file_path), f"temp_{safe_name}")

    try:
        result = subprocess.run(
            ["hwp5html", "--output", html_dir, file_path],
            capture_output=True, text=True, check=False,
        )
        if result.returncode != 0:
            logger.error(
                "HWP 변환 실패 | file=%s rc=%s err=%s",
                file_path, result.returncode, (result.stderr or "").strip(),
            )
            return 0

        index_html = os.path.join(html_dir, "index.xhtml")
        if not os.path.exists(index_html):
            logger.error("index.xhtml 없음 | file=%s", file_path)
            return 0

        soup = BeautifulSoup(read_text_with_fallbacks(index_html), "html.parser")

        table_count = 0
        for i, table in enumerate(soup.find_all("table")):
            rows = table.find_all("tr")
            table_data = [
                [col.get_text(strip=True) for col in row.find_all(["td", "th"])]
                for row in rows
            ]
            table_data = [r for r in table_data if any(r)]
            if len(table_data) < 2:
                continue
            try:
                max_cols = max(len(r) for r in table_data)
                normalized = [r + [None] * (max_cols - len(r)) for r in table_data]
                df = _parse_table(normalized)
                if df is None:
                    continue
                tbl = f"{safe_name}_html_t{i}"
                with engine.begin() as conn:
                    _drop_table_and_type(conn, tbl)
                df.to_sql(tbl, engine, if_exists="fail", index=False)
                logger.info("[HWP] 표 저장 | tbl=%s rows=%d", tbl, len(df))
                table_count += 1
            except Exception:
                logger.exception("[HWP] 표 저장 실패 | file=%s t=%d", file_path, i)

        for tag in soup.find_all("table"):
            tag.decompose()

        body_text = soup.get_text(separator="\n")
        chunk_records = split_into_chunks(body_text)

        chroma_count = save_to_chroma(file_path, chunk_records, file_hash, category) if chunk_records else 0
        logger.info("HWP 완료 | file=%s tables=%d chunks=%d", file_path, table_count, chroma_count)
        return chroma_count

    finally:
        if os.path.exists(html_dir):
            shutil.rmtree(html_dir, ignore_errors=True)

# ---------------------------------------------------------------------------
# 단일 파일 처리 진입점
# ---------------------------------------------------------------------------
def _cleanup_stale_hwp_tables(safe_name: str):
    """PDF로 교체된 파일의 잔여 HWP html_t 테이블 삭제."""
    drop_tables_with_prefix(f"{safe_name}_html_t")


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

    try:
        chroma_doc_count = 0

        if ext == "xlsx":
            ingest_xlsx_to_postgres(file_path)
        elif ext == "pdf":
            safe_name = sanitize_table_name(os.path.basename(file_path).rsplit(".", 1)[0])
            _cleanup_stale_hwp_tables(safe_name)
            chroma_doc_count = ingest_pdf_hybrid(file_path, file_hash, category)
        elif ext == "hwp":
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
    import glob

    ensure_manifest_table()

    data_folder = os.path.join(os.path.dirname(__file__), "..", "data")
    if not os.path.exists(data_folder):
        print(f"'{data_folder}' 폴더가 없습니다.")
        sys.exit(1)

    file_paths = []
    for ext in ("xlsx", "pdf", "hwp"):
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
