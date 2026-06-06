from __future__ import annotations

import hashlib
import logging
import re

import pandas as pd

logger = logging.getLogger("ingest")

# 집계 행 탐지 패턴: 셀 값이 "합계", "소계", "계", "장학금 계" 등
_AGGREGATE_ROW_RE = re.compile(
    r"^(합\s*계|소\s*계|총\s*계|합\s*산|계|장학금\s*계|total|subtotal)$",
    re.IGNORECASE,
)
# 금액 공식 패턴: "N명*N만원" 형식의 요약 수식
_AMOUNT_FORMULA_RE = re.compile(r"\d+명\s*[*×x]\s*\d+", re.IGNORECASE)


def _cell_val(cell) -> str:
    return str(cell).strip() if cell is not None else ""


def sanitize_table_name(name: str) -> str:
    original = name
    name = re.sub(r"[^\x00-\x7F]", "", name)   # 한글 등 non-ASCII 제거
    name = re.sub(r"[^a-zA-Z0-9]", "_", name)  # 특수문자 → _
    name = re.sub(r"_+", "_", name).strip("_") # 연속 _ 정리
    name = name.lower()[:32].rstrip("_")        # 소문자 + 32자 제한
    if not name:
        name = "tbl_" + hashlib.md5(original.encode("utf-8")).hexdigest()[:8]
    elif name[0].isdigit():
        name = "tbl_" + name
    return name


def sanitize_column_name(col: str) -> str | None:
    col = str(col).strip()
    if not col or col in ("None", "nan"):
        return None
    col = re.sub(r"[^\w가-힣]", "_", col, flags=re.UNICODE)
    col = re.sub(r"_+", "_", col).strip("_")
    col = col[:40]
    if not col:
        return None
    if col[0].isdigit():
        col = "col_" + col
    return col


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """인제스트 시 공통 정제: 집계 행 제거 + 컬럼값 정규화."""
    if df is None or df.empty:
        return df

    # 1. 집계 행 제거
    agg_mask = pd.Series(False, index=df.index)
    for col in df.columns:
        vals = df[col].astype(str).str.strip()
        agg_mask |= vals.apply(
            lambda v: bool(_AGGREGATE_ROW_RE.match(v)) or bool(_AMOUNT_FORMULA_RE.search(v))
        )

    if agg_mask.any():
        logger.info("집계 행 제거: %d행", int(agg_mask.sum()))
        df = df[~agg_mask].reset_index(drop=True)

    # 2. 숫자/금액만 있는 footer 행 제거
    _DIGIT_AMOUNT_RE = re.compile(r'^\d[\d,]*$|^\d[\d,]*만원$|^\d[\d,]*원$')

    def _is_footer_row(row: pd.Series) -> bool:
        vals = [str(v).strip() for v in row if str(v).strip() not in ("", "None", "nan")]
        if len(vals) < 2:
            return False
        non_amount = [v for v in vals if not _DIGIT_AMOUNT_RE.match(v)]
        if any(re.search(r'[가-힣]', v) for v in non_amount):
            return False
        return all(_DIGIT_AMOUNT_RE.match(v) for v in vals) and len(set(vals)) <= 3

    footer_mask = df.apply(_is_footer_row, axis=1)
    if footer_mask.any():
        logger.info("숫자전용 footer 행 제거: %d행", int(footer_mask.sum()))
        df = df[~footer_mask].reset_index(drop=True)

    # 3. 말미 중복 순번 행 제거
    seq_col = next(
        (c for c in df.columns if any(k in c for k in ("연번", "순번", "번호", "순"))), None
    )
    if seq_col:
        try:
            while len(df) > 1:
                seq_vals = pd.to_numeric(df[seq_col], errors="coerce")
                last, prev = seq_vals.iloc[-1], seq_vals.iloc[-2]
                if pd.notna(last) and pd.notna(prev) and last == prev:
                    logger.info("말미 중복 순번 행 제거: %s=%s", seq_col, df.iloc[-1][seq_col])
                    df = df.iloc[:-1].reset_index(drop=True)
                else:
                    break
        except Exception:
            pass

    # 4. 학과·계열 컬럼 값에서 "(N명)" suffix 제거
    for col in df.columns:
        if any(k in col for k in ("학과", "계열", "학부", "전공", "대상학생")):
            try:
                cleaned = df[col].astype(str).str.replace(r"\(\d+명\)", "", regex=True).str.strip()
                df[col] = cleaned.where(~cleaned.isin({"None", "nan", ""}), None)
            except Exception:
                pass

    return df


def _parse_table(raw_table: list[list]) -> "pd.DataFrame | None":
    """병합 셀(None) 처리 + 2행 헤더 자동 탐지 후 DataFrame 반환."""
    if not raw_table or len(raw_table) < 2:
        return None

    ncols = max(len(r) for r in raw_table)
    table = [list(r) + [None] * (ncols - len(r)) for r in raw_table]

    header_idx = 0
    for i, row in enumerate(table):
        if sum(1 for c in row if _cell_val(c)) >= ncols * 0.4:
            header_idx = i
            break

    h1 = table[header_idx]
    data_start = header_idx + 1

    if data_start < len(table):
        h2 = table[data_start]
        empty_pos = [j for j in range(ncols) if not _cell_val(h1[j])]
        fills = sum(1 for j in empty_pos if _cell_val(h2[j]))
        if empty_pos and fills >= len(empty_pos) * 0.5:
            merged = [_cell_val(h2[j]) if not _cell_val(h1[j]) else _cell_val(h1[j])
                      for j in range(ncols)]
            data_start += 1
        else:
            merged = [_cell_val(c) for c in h1]
    else:
        merged = [_cell_val(c) for c in h1]

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
    df = df.ffill(axis=0)
    df = df.dropna(how="all").replace("\n", " ", regex=True)
    if df.empty:
        return None
    df = _clean_dataframe(df)
    return df if not df.empty else None
