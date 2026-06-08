from __future__ import annotations

import re

from langchain_text_splitters import RecursiveCharacterTextSplitter

CHUNK_SIZE    = 500
CHUNK_OVERLAP = 100
MIN_CHUNK_LEN = 20

_splitter = RecursiveCharacterTextSplitter(
    chunk_size=CHUNK_SIZE,
    chunk_overlap=CHUNK_OVERLAP,
    separators=["\n\n", "\n", ". ", " ", ""],
)

_FILENAME_AMOUNT_RE = re.compile(r"(\d[\d,]*)만원")


def clean_pdf_text(raw: str) -> str:
    raw = re.sub(r"([^\s])-\n([^\s])", r"\1\2", raw)
    raw = re.sub(r"\n{3,}", "\n\n", raw)
    raw = re.sub(r"^\s*\d+\s*$", "", raw, flags=re.MULTILINE)
    return raw.strip()


def split_into_chunks(raw: str, page: int | None = None) -> list[dict]:
    return [
        {"text": c, "page": page}
        for c in _splitter.split_text(raw)
        if len(c.strip()) >= MIN_CHUNK_LEN
    ]


def _table_to_text_chunks(df, doc_label: str, page: int | None = None) -> list[dict]:
    """Context Padding: 각 행을 '컬럼명: 값' 쌍으로 직렬화하여 문맥 보존."""
    cols = [c for c in df.columns]
    if not cols:
        return []

    chunks = []
    for _, row in df.iterrows():
        parts = [f"[문서: {doc_label}]"]
        for c in cols:
            v = row[c]
            s = str(v).strip() if v is not None and str(v) not in ("None", "nan") else None
            if s:
                parts.append(f"{c}: {s}")
        text = " / ".join(parts)
        if len(text) >= MIN_CHUNK_LEN:
            chunks.append({"text": text, "page": page})
    return chunks


def _make_doc_overview_chunk(doc_label: str, source_file: str, dfs: list) -> "dict | None":
    """문서 개요 청크: 목적·내용 질문에 대한 벡터 검색용."""
    total_rows = sum(len(d) for d in dfs)
    all_cols: list[str] = []
    for d in dfs:
        for c in d.columns:
            if c not in all_cols:
                all_cols.append(c)

    lines = [
        "[문서 개요]",
        f"문서명: {doc_label}",
        f"파일: {source_file}",
    ]
    m = _FILENAME_AMOUNT_RE.search(source_file)
    if m:
        lines.append(f"총 지원 금액: {m.group(1)}만원")
    if total_rows:
        lines.append(f"데이터: 총 {total_rows}건")
    if all_cols:
        lines.append(f"항목: {', '.join(all_cols[:8])}")

    core = re.sub(r"\s*[-–]\s*\d[\d,]*만원.*$", "", doc_label)
    core = re.sub(r"\s*\([^)]*\)\s*", " ", core).strip()
    core = re.sub(r"^\d+\.\s*", "", core).strip()
    core = re.sub(r"\s+", " ", core)
    if core:
        lines.append(f"목적: 이 문서는 {core}에 관한 명단 및 관련 정보를 담고 있습니다.")

    text = "\n".join(lines)
    return {"text": text, "page": None} if len(text) >= MIN_CHUNK_LEN else None
