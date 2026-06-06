from __future__ import annotations

import json
import logging
import os

import pandas as pd

from utils.parquet_store import DATAFRAME_DIR

logger = logging.getLogger("uvicorn.error")

# ---------------------------------------------------------------------------
# 공유 mutable state — 다른 파일에서 직접 임포트해 참조 공유
# ---------------------------------------------------------------------------
_df_namespace: dict[str, pd.DataFrame] = {}   # var_name → DataFrame
_df_sources:   dict[str, str]          = {}   # var_name → 원본 파일명
_df_labels:    dict[str, str]          = {}   # var_name → 표시용 레이블
_df_schema_cache: tuple[str, float] | None = None
_SCHEMA_CACHE_TTL = 300


def _load_dataframes():
    """dataframes/ 폴더의 Parquet 파일을 모두 메모리에 로드한다.
    변수명은 df0, df1, df2 ... 형태로 단순화해 LLM이 잘못 잘라 쓰는 것을 방지한다."""
    global _df_namespace, _df_sources, _df_labels, _df_schema_cache
    _df_namespace.clear()
    _df_sources.clear()
    _df_labels.clear()
    _df_schema_cache = None

    if not os.path.exists(DATAFRAME_DIR):
        return

    entries = []
    for fname in sorted(os.listdir(DATAFRAME_DIR)):
        if not fname.endswith(".parquet"):
            continue
        orig_name = fname[:-len(".parquet")]
        path      = os.path.join(DATAFRAME_DIR, fname)
        meta_path = os.path.join(DATAFRAME_DIR, f"{orig_name}.meta.json")
        try:
            df     = pd.read_parquet(path)
            source = orig_name
            label  = orig_name
            if os.path.exists(meta_path):
                with open(meta_path, encoding="utf-8") as f:
                    meta = json.load(f)
                source = meta.get("source", orig_name)
                label  = meta.get("label", orig_name)
            # 학과 관련 컬럼의 "(N명)" suffix 제거 — LLM 혼란 방지
            for col in df.columns:
                if any(k in col for k in ("학과", "계열", "대상학생")):
                    try:
                        df[col] = df[col].astype(str).str.replace(r"\(\d+명\)", "", regex=True).str.strip()
                    except Exception:
                        pass
            entries.append((df, source, label))
        except Exception as e:
            logger.warning("DataFrame 로드 실패 | file=%s err=%s", fname, e)

    # df0, df1, df2 ... 로 단순 명명 — LLM이 긴 파일명 기반 변수명을 잘못 잘라 쓰는 문제 방지
    for i, (df, source, label) in enumerate(entries):
        alias = f"df{i}"
        _df_namespace[alias] = df
        _df_sources[alias]   = source
        _df_labels[alias]    = label

    logger.info("DataFrame %d개 로드 완료", len(_df_namespace))
