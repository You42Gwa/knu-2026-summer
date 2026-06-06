from __future__ import annotations

import logging
import os

import pdfplumber

from utils.table_parser import _parse_table, sanitize_table_name
from utils.text_utils import _table_to_text_chunks, _make_doc_overview_chunk, clean_pdf_text, split_into_chunks
from utils.parquet_store import save_dataframe, drop_dataframe_files
from utils.chroma_store import save_to_chroma

# OCR 선택적 임포트
try:
    from pdf2image import convert_from_path
    import pytesseract
    HAS_OCR = True
except ImportError:
    HAS_OCR = False

logger = logging.getLogger("ingest")

OCR_DPI  = 300
OCR_LANG = "kor+eng"


def _extract_page_texts(file_path: str) -> dict[int, str]:
    page_texts: dict[int, str] = {}
    scanned_pages: list[int] = []

    with pdfplumber.open(file_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            try:
                table_bboxes = [tbl.bbox for tbl in page.find_tables()]

                if table_bboxes:
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


def ingest_pdf_hybrid(file_path: str, file_hash: str, category: str) -> int:
    logger.info("[PDF] %s", file_path)

    safe_name   = sanitize_table_name(os.path.basename(file_path).rsplit(".", 1)[0])
    source_file = os.path.basename(file_path)
    doc_label   = os.path.splitext(source_file)[0]

    drop_dataframe_files(f"df_{safe_name}_p")

    page_texts = _extract_page_texts(file_path)

    chunk_records: list[dict] = []
    parsed_tables: list[pd.DataFrame] = []
    for page_num, raw_text in page_texts.items():
        cleaned = clean_pdf_text(raw_text)
        chunk_records.extend(split_into_chunks(cleaned, page=page_num))

    import pandas as pd

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
                    parsed_tables.append(df)
                    var_name = f"df_{safe_name}_p{page_num}t{table_count}"
                    label    = f"{doc_label} (p.{page_num} 표{table_count + 1})"
                    save_dataframe(df, var_name, source_file, label)
                    logger.info("[PDF] 표 저장 | var=%s rows=%d", var_name, len(df))
                    table_count += 1
                    chunk_records.extend(_table_to_text_chunks(df, doc_label, page_num))
                except Exception:
                    logger.exception("[PDF] 표 저장 실패 | page=%d t=%d", page_num, t_idx)

    if parsed_tables:
        overview = _make_doc_overview_chunk(doc_label, source_file, parsed_tables)
        if overview:
            chunk_records.insert(0, overview)

    if not chunk_records and table_count == 0:
        logger.warning("추출 데이터 없음 | file=%s", file_path)

    chroma_count = save_to_chroma(file_path, chunk_records, file_hash, category) if chunk_records else 0
    logger.info("PDF 완료 | file=%s tables=%d chunks=%d", file_path, table_count, chroma_count)
    return chroma_count
