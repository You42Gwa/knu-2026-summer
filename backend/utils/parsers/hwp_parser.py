from __future__ import annotations

import json
import logging
import os
import subprocess
import sys

import pandas as pd

from utils.table_parser import _parse_table, _clean_dataframe, sanitize_table_name
from utils.text_utils import _table_to_text_chunks, _make_doc_overview_chunk
from utils.parquet_store import save_dataframe, drop_dataframe_files
from utils.chroma_store import save_to_chroma

logger = logging.getLogger("ingest")


def _extract_hwp_table_pyhwpx(file_path: str) -> "pd.DataFrame | None":
    """hwp_extract.py를 subprocess로 실행해 COM 스레드 격리."""
    helper = os.path.join(os.path.dirname(os.path.dirname(__file__)), "hwp_extract.py")
    try:
        result = subprocess.run(
            [sys.executable, helper, file_path],
            capture_output=True, timeout=60,
        )
        stdout = result.stdout.decode("utf-8", errors="replace").strip()
        if result.returncode != 0 or not stdout:
            stderr = result.stderr.decode("utf-8", errors="replace").strip()
            logger.error("[HWP/pyhwpx] subprocess 실패 | rc=%d err=%s", result.returncode, stderr)
            return None
        records = json.loads(stdout)
        if not records:
            return None
        df = pd.DataFrame(records)
        return df if not df.empty else None
    except Exception as e:
        logger.error("[HWP/pyhwpx] 표 추출 실패 | file=%s err=%s", file_path, e)
        return None


def convert_hwp_to_html_and_ingest(file_path: str, file_hash: str, category: str) -> int:
    logger.info("[HWP] %s", file_path)

    safe_name   = sanitize_table_name(os.path.basename(file_path).rsplit(".", 1)[0])
    source_file = os.path.basename(file_path)
    doc_label   = os.path.splitext(source_file)[0]

    drop_dataframe_files(f"df_{safe_name}_t")

    df_pyhwpx = _extract_hwp_table_pyhwpx(file_path)
    if df_pyhwpx is not None:
        df_cleaned = _clean_dataframe(df_pyhwpx)
        if df_cleaned is not None and not df_cleaned.empty:
            var_name = f"df_{safe_name}_t0"
            save_dataframe(df_cleaned, var_name, source_file, doc_label)
            overview = _make_doc_overview_chunk(doc_label, source_file, [df_cleaned])
            chunks = _table_to_text_chunks(df_cleaned, doc_label)
            if overview:
                chunks.insert(0, overview)
            count = save_to_chroma(file_path, chunks, file_hash, category) if chunks else 0
            logger.info("[HWP/pyhwpx] 완료 | file=%s rows=%d chunks=%d", file_path, len(df_cleaned), count)
            return count
    logger.error("HWP 변환 실패 | file=%s", file_path)
    return 0
