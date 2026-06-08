from __future__ import annotations

import logging
import math
import os

import pandas as pd

from utils.table_parser import _parse_table, sanitize_table_name
from utils.text_utils import _table_to_text_chunks, _make_doc_overview_chunk
from utils.parquet_store import save_dataframe, drop_dataframe_files
from utils.chroma_store import save_to_chroma

logger = logging.getLogger("ingest")


def ingest_xlsx(file_path: str, file_hash: str = "", category: str = "") -> int:
    logger.info("[XLSX] %s", file_path)
    base_name   = sanitize_table_name(os.path.basename(file_path).rsplit(".", 1)[0])
    source_file = os.path.basename(file_path)
    doc_label   = os.path.splitext(source_file)[0]

    drop_dataframe_files(f"df_{base_name}")

    xl = pd.ExcelFile(file_path, engine="openpyxl")
    sheets = xl.sheet_names
    all_chunk_records: list[dict] = []
    parsed_tables: list[pd.DataFrame] = []

    for i, sheet_name in enumerate(sheets):
        raw_df = xl.parse(sheet_name, header=None)
        if raw_df.empty:
            logger.info("빈 시트 건너뜀 | sheet=%s", sheet_name)
            continue

        raw_table = [
            [None if (v is None or (isinstance(v, float) and math.isnan(v))) else v
             for v in row]
            for row in raw_df.values.tolist()
        ]
        df = _parse_table(raw_table)
        if df is None:
            logger.warning("XLSX 파싱 결과 없음 | sheet=%s", sheet_name)
            continue

        parsed_tables.append(df)
        var_name = f"df_{base_name}_s{i}" if len(sheets) > 1 else f"df_{base_name}"
        label    = f"{doc_label} - {sheet_name}" if len(sheets) > 1 else doc_label
        save_dataframe(df, var_name, source_file, label)
        logger.info("[XLSX] '%s' 저장 완료 | sheet=%s rows=%d", var_name, sheet_name, len(df))

        all_chunk_records.extend(_table_to_text_chunks(df, doc_label))

    if parsed_tables:
        overview = _make_doc_overview_chunk(doc_label, source_file, parsed_tables)
        if overview:
            all_chunk_records.insert(0, overview)

    if all_chunk_records and file_hash:
        count = save_to_chroma(file_path, all_chunk_records, file_hash, category)
        logger.info("[XLSX] Chroma 저장 완료 | chunks=%d", count)
        return count
    return 0
