from __future__ import annotations

import json
import logging
import os

import pandas as pd

# DATAFRAME_DIR: backend/dataframes/
DATAFRAME_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "dataframes",
)

logger = logging.getLogger("ingest")


def save_dataframe(df: pd.DataFrame, var_name: str, source_file: str, label: str = "") -> str:
    """DataFrame을 Parquet으로 저장하고 메타데이터를 함께 기록한다."""
    os.makedirs(DATAFRAME_DIR, exist_ok=True)
    path = os.path.join(DATAFRAME_DIR, f"{var_name}.parquet")
    df.to_parquet(path, index=False)

    meta_path = os.path.join(DATAFRAME_DIR, f"{var_name}.meta.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(
            {"source": source_file, "label": label or var_name, "rows": len(df)},
            f,
            ensure_ascii=False,
        )

    logger.info("DataFrame 저장 | var=%s rows=%d", var_name, len(df))
    return path


def drop_dataframe_files(prefix: str):
    """prefix와 정확히 일치하거나 prefix_ 로 시작하는 parquet/meta 파일을 삭제한다."""
    if not os.path.exists(DATAFRAME_DIR):
        return
    for fname in os.listdir(DATAFRAME_DIR):
        if not (fname.endswith(".parquet") or fname.endswith(".meta.json")):
            continue
        stem = fname
        for ext in (".parquet", ".meta.json"):
            if stem.endswith(ext):
                stem = stem[: -len(ext)]
                break
        if stem == prefix or stem.startswith(prefix + "_"):
            fpath = os.path.join(DATAFRAME_DIR, fname)
            os.remove(fpath)
            logger.info("DataFrame 파일 삭제: %s", fname)


def drop_dataframe_by_source(source: str) -> int:
    """meta.json의 source 필드를 기준으로 parquet·meta 파일을 삭제한다. 삭제된 쌍 수를 반환."""
    if not os.path.exists(DATAFRAME_DIR):
        return 0
    targets: list[str] = []
    for fname in os.listdir(DATAFRAME_DIR):
        if not fname.endswith(".meta.json"):
            continue
        meta_path = os.path.join(DATAFRAME_DIR, fname)
        try:
            with open(meta_path, encoding="utf-8") as f:
                meta = json.load(f)
            if os.path.basename(meta.get("source", "")) == os.path.basename(source):
                stem = fname[: -len(".meta.json")]
                targets.append(stem)
        except Exception:
            continue
    for stem in targets:
        for ext in (".parquet", ".meta.json"):
            fpath = os.path.join(DATAFRAME_DIR, stem + ext)
            if os.path.exists(fpath):
                os.remove(fpath)
                logger.info("DataFrame 파일 삭제: %s", stem + ext)
    return len(targets)
