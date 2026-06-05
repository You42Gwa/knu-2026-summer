"""
골드셋 자동 생성 스크립트 — 약 100케이스 목표

사용법:
    python make_goldset.py                  # 전체 파일 처리
    python make_goldset.py --dry-run        # goldset.json 미저장, 콘솔 출력만
    python make_goldset.py --skip-existing  # 이미 source_docs에 있는 파일 건너뜀
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import pdfplumber

DATA_DIR     = Path(__file__).parent.parent / "data"
GOLDSET_PATH = Path(__file__).parent / "goldset.json"

_PARSEABLE_EXTS = {".pdf", ".xlsx", ".xls"}

# ---------------------------------------------------------------------------
# 정렬
# ---------------------------------------------------------------------------
_NUM_RE = re.compile(r"^(\d+)\.")

def _sort_key(p: Path) -> int:
    m = _NUM_RE.match(p.name)
    return int(m.group(1)) if m else 9999

def find_data_files() -> list[Path]:
    files = []
    for ext in ("pdf", "xlsx", "xls"):
        files.extend(DATA_DIR.glob(f"*.{ext}"))
    return sorted(files, key=_sort_key)

# ---------------------------------------------------------------------------
# 추출 유틸
# ---------------------------------------------------------------------------
def _clean(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() in ("nan", "none", "") else s

def read_xlsx(path: Path) -> list[dict]:
    xl = pd.ExcelFile(path, engine="openpyxl")
    return [{"sheet": s, "df": xl.parse(s, header=None, dtype=str).fillna("").replace("nan", "")}
            for s in xl.sheet_names]

def read_xlsx_description(path: Path) -> str:
    """Excel A2 셀(설명 텍스트) 추출."""
    try:
        xl = pd.ExcelFile(path, engine="openpyxl")
        df = xl.parse(xl.sheet_names[0], header=None, dtype=str).fillna("")
        if len(df) >= 2:
            val = str(df.iloc[1, 0]).strip()
            return val if val not in ("nan", "none", "") else ""
    except Exception:
        pass
    return ""

def read_pdf_tables(path: Path) -> list[dict]:
    tables = []
    with pdfplumber.open(path) as pdf:
        for page_num, page in enumerate(pdf.pages, 1):
            for tbl in page.extract_tables() or []:
                tables.append({"page": page_num, "table": tbl})
    return tables

def read_pdf_text(path: Path) -> str:
    lines = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                lines.append(t.strip())
    return "\n".join(lines)

# ---------------------------------------------------------------------------
# 파일명 메타
# ---------------------------------------------------------------------------
def parse_filename_meta(name: str) -> dict:
    meta = {"amount": None, "month": None}
    m = re.search(r"(\d[\d,]+)만원", name)
    if m:
        meta["amount"] = m.group(0)
    m2 = re.search(r"(\d{1,2})월", name)
    if m2:
        meta["month"] = f"{m2.group(1)}월"
    return meta

# ---------------------------------------------------------------------------
# 이름·학과·학년 추출
# ---------------------------------------------------------------------------
_NAME_BLACKLIST = {
    "연번", "학과", "성명", "학년", "번호", "비고", "계열", "파트",
    "대상", "학생", "생년", "월일", "연락", "합계", "기계", "전기",
    "화학", "자동차", "건축", "건설", "생산", "공간", "순번", "이름",
    "반", "명", "부서", "직급", "소속", "구분", "항목", "내용",
    "금액", "지급", "수령", "확인", "서명", "날짜", "담당", "승인",
    "학교", "교장", "교감", "선생", "교사", "강사", "입학", "졸업",
    "종목", "선수", "코치", "감독", "부장", "위원", "회장", "총무",
    "생년월일", "대상학생", "연락처", "명단", "목록", "리스트",
    "장학금", "동문회", "수혜자", "대상자", "지급액", "총액", "지원금",
    "클라리넷", "플륫", "트롬본", "튜바", "트럼펫", "오보에",
    "플루트", "호른", "색소폰", "타악기", "퍼커션",
    "화공", "자공", "학반", "학번", "반번", "출결", "지급처",
    "현재", "변경", "이전", "상태", "조건", "성별", "남자", "여자",
}
_NON_NAME_SUFFIX = re.compile(r"(과|계열|반|중|처|실|실장|부|부장|위|원|회|관|소|원장|장|팀|재단|학교)$")

def _is_name(s: str) -> bool:
    if not re.fullmatch(r"[가-힣]{2,4}", s):
        return False
    if s in _NAME_BLACKLIST:
        return False
    if _NON_NAME_SUFFIX.search(s):
        return False
    return True

def extract_names_from_rows(rows: list[list]) -> list[str]:
    names = []
    for row in rows:
        for cell in row:
            v = _clean(cell)
            if _is_name(v) and v not in names:
                names.append(v)
    return names

def extract_names_from_df(df: pd.DataFrame) -> list[str]:
    names = []
    for col in df.columns:
        for v in df[col]:
            s = _clean(v)
            if _is_name(s) and s not in names:
                names.append(s)
    return names

def extract_departments_from_rows(rows: list[list]) -> list[str]:
    deps = []
    for row in rows:
        for cell in row:
            v = _clean(cell)
            if re.search(r"(과|계열|부)$", v) and len(v) >= 3 and v not in deps:
                deps.append(v)
    return deps

def extract_departments_from_df(df: pd.DataFrame) -> list[str]:
    deps = []
    for col in df.columns:
        for v in df[col]:
            s = _clean(v)
            if re.search(r"(과|계열|부)$", s) and len(s) >= 3 and s not in deps:
                deps.append(s)
    return deps

def _extract_grades_from_rows(rows: list[list]) -> list[str]:
    grades = []
    for row in rows:
        for cell in row:
            v = _clean(cell)
            if re.fullmatch(r"[1-4]", v) and v not in grades:
                grades.append(v)
    return grades

def _extract_grades_from_df(df: pd.DataFrame) -> list[str]:
    grades = []
    for col in df.columns:
        for v in df[col]:
            s = _clean(v)
            if re.fullmatch(r"[1-4]", s) and s not in grades:
                grades.append(s)
    return grades

# ---------------------------------------------------------------------------
# 헬퍼
# ---------------------------------------------------------------------------
TC_ID_COUNTER = [0]

def _next_id() -> str:
    TC_ID_COUNTER[0] += 1
    return f"TC{TC_ID_COUNTER[0]:03d}"

def _doc_label(fname: str) -> str:
    name = re.sub(r"^\d+\.\s*", "", fname)
    name = re.sub(r"\.\w{2,5}$", "", name)
    name = re.sub(r"\s*-?\s*\d[\d,]*만원", "", name)
    name = re.sub(r"\s*\d{1,2}월", "", name)
    name = re.sub(r"\([^)]*\)", "", name).strip()
    name = re.sub(r"\d{4}학년도\s*", "", name)
    name = re.sub(r"\d{4}\.\s*", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name if name else fname

def _keywords_from_desc(desc: str) -> list[str]:
    """설명 텍스트에서 핵심 키워드 추출."""
    patterns = [
        r"학업 의욕 고취", r"성취 동기", r"경제적 어려움", r"경제적 지원",
        r"균등 지급", r"차등 지급", r"차등 선발", r"성적 우수",
        r"신입생 전원", r"체육 활동", r"종합 심사",
        r"동문회", r"체육부", r"장학재단", r"한빛",
        r"지급 목적", r"선발 기준", r"지급 기관",
    ]
    found = []
    for pat in patterns:
        if re.search(pat, desc):
            m = re.search(pat, desc)
            found.append(m.group(0))
    return found[:5] if found else []

# ---------------------------------------------------------------------------
# 케이스 생성 (파일 단위)
# ---------------------------------------------------------------------------
def _build_cases(fname: str, names: list[str], deps: list[str],
                 grades: list[str], meta: dict,
                 description: str = "", text: str = "") -> list[dict]:
    label = _doc_label(fname)
    cases: list[dict] = []
    desc_kw = _keywords_from_desc(description or text)

    # ── SQL: 명단 ────────────────────────────────────────────────
    if names:
        cases.append({
            "id": _next_id(), "question": f"{label} 명단 알려줘",
            "category": "sql_명단", "expected_route": "sql",
            "ground_truth_keywords": names[:5],
            "ground_truth_note": f"'{fname}' 첫 5명 샘플",
            "source_docs": [fname], "difficulty": "easy",
        })

    # ── SQL: 학과별 명단 ─────────────────────────────────────────
    if deps:
        cases.append({
            "id": _next_id(), "question": f"{deps[0]} 명단 알려줘",
            "category": "sql_명단", "expected_route": "sql",
            "ground_truth_keywords": [deps[0]] + names[:2],
            "ground_truth_note": f"'{fname}' {deps[0]} 소속 명단",
            "source_docs": [fname], "difficulty": "easy",
        })

    # ── SQL: 인원 ────────────────────────────────────────────────
    if names:
        cases.append({
            "id": _next_id(), "question": f"{label} 총 인원은 몇 명이야",
            "category": "sql_인원", "expected_route": "sql",
            "ground_truth_keywords": [f"{len(names)}명"],
            "ground_truth_note": f"추출 이름 기준 {len(names)}명",
            "source_docs": [fname], "difficulty": "easy",
        })

    if deps:
        cases.append({
            "id": _next_id(), "question": f"{label}에서 {deps[0]}은 몇 명이야",
            "category": "sql_인원", "expected_route": "sql",
            "ground_truth_keywords": [deps[0]],
            "ground_truth_note": f"'{fname}' {deps[0]} 인원수",
            "source_docs": [fname], "difficulty": "medium",
        })

    # ── SQL: 특정 학생 ───────────────────────────────────────────
    if len(names) >= 3:
        pick1 = names[2]
        cases.append({
            "id": _next_id(), "question": f"{pick1} 학생이 {label}에 있어",
            "category": "sql_명단", "expected_route": "sql",
            "ground_truth_keywords": [pick1],
            "ground_truth_note": f"'{fname}'에 {pick1} 존재 여부",
            "source_docs": [fname], "difficulty": "easy",
        })

    # 없는 이름 엣지케이스
    cases.append({
        "id": _next_id(), "question": f"나도없어 학생이 {label} 받았어",
        "category": "sql_명단", "expected_route": "sql",
        "ground_truth_keywords": ["없", "조회된 데이터가 없"],
        "ground_truth_note": "존재하지 않는 이름 → 없음 반환 확인",
        "source_docs": [fname], "difficulty": "easy",
    })

    # ── SQL: 금액 ────────────────────────────────────────────────
    if meta["amount"]:
        cases.append({
            "id": _next_id(), "question": f"{label} 총 지급 금액은 얼마야",
            "category": "sql_금액", "expected_route": "sql",
            "ground_truth_keywords": [meta["amount"]],
            "ground_truth_note": f"파일명 기준 총액 {meta['amount']}",
            "source_docs": [fname], "difficulty": "easy",
        })

    if len(names) >= 2:
        pick2 = names[1]
        cases.append({
            "id": _next_id(), "question": f"{pick2}는 {label} 얼마 받았어",
            "category": "sql_금액", "expected_route": "sql",
            "ground_truth_keywords": [pick2],
            "ground_truth_note": f"'{fname}'에서 {pick2}의 금액",
            "source_docs": [fname], "difficulty": "medium",
        })

    if names:
        cases.append({
            "id": _next_id(), "question": f"{label}에서 가장 많이 받은 학생은 얼마야",
            "category": "sql_금액", "expected_route": "sql",
            "ground_truth_keywords": ["최대", "최고"],
            "ground_truth_note": f"'{fname}' 최고 지급액 학생",
            "source_docs": [fname], "difficulty": "medium",
        })

    # ── Vector: 설명 텍스트 기반 ─────────────────────────────────
    desc_text = description or text
    if desc_text.strip():
        # 11. 문서 목적/내용 설명
        cases.append({
            "id": _next_id(), "question": f"{label} 문서의 목적이나 내용을 설명해줘",
            "category": "vector_문서", "expected_route": "vector",
            "ground_truth_keywords": desc_kw or [label],
            "ground_truth_note": "설명 텍스트 기반 — 목적·기준 키워드 포함 여부",
            "source_docs": [fname], "difficulty": "easy",
        })
        # 12. 지급 목적
        cases.append({
            "id": _next_id(), "question": f"{label} 지급 목적이 뭐야",
            "category": "vector_문서", "expected_route": "vector",
            "ground_truth_keywords": desc_kw[:3] if desc_kw else [label],
            "ground_truth_note": "설명 텍스트에서 지급 목적 추출",
            "source_docs": [fname], "difficulty": "easy",
        })
        # 13. 선발 기준
        cases.append({
            "id": _next_id(), "question": f"{label} 어떤 기준으로 선발했어",
            "category": "vector_문서", "expected_route": "vector",
            "ground_truth_keywords": desc_kw[:3] if desc_kw else [label],
            "ground_truth_note": "설명 텍스트에서 선발 기준 추출",
            "source_docs": [fname], "difficulty": "medium",
        })
        # 14. 지급 기관
        cases.append({
            "id": _next_id(), "question": f"{label} 어디서 지급한 장학금이야",
            "category": "vector_문서", "expected_route": "vector",
            "ground_truth_keywords": desc_kw[:2] if desc_kw else [label],
            "ground_truth_note": "설명 텍스트에서 지급 기관 추출",
            "source_docs": [fname], "difficulty": "easy",
        })

    return cases


# ---------------------------------------------------------------------------
# PDF / XLSX 진입점
# ---------------------------------------------------------------------------
def cases_from_pdf(path: Path) -> list[dict]:
    fname  = path.name
    meta   = parse_filename_meta(fname)
    tables = read_pdf_tables(path)
    text   = read_pdf_text(path)

    all_rows: list[list] = []
    for t in tables:
        all_rows.extend(t["table"])

    names  = extract_names_from_rows(all_rows)
    deps   = extract_departments_from_rows(all_rows)
    grades = _extract_grades_from_rows(all_rows)

    print(f"  → 테이블 {len(tables)}개 | 이름 {len(names)}개 | 학과 {len(deps)}개 | 학년 {grades}")
    if names:
        print(f"     이름 샘플: {names[:6]}")

    return _build_cases(fname, names, deps, grades, meta, text=text)


def cases_from_xlsx(path: Path) -> list[dict]:
    fname   = path.name
    meta    = parse_filename_meta(fname)
    sheets  = read_xlsx(path)
    desc    = read_xlsx_description(path)

    all_names:  list[str] = []
    all_deps:   list[str] = []
    all_grades: list[str] = []
    for s in sheets:
        all_names.extend(extract_names_from_df(s["df"]))
        all_deps.extend(extract_departments_from_df(s["df"]))
        all_grades.extend(_extract_grades_from_df(s["df"]))

    all_names  = list(dict.fromkeys(all_names))
    all_deps   = list(dict.fromkeys(all_deps))
    all_grades = list(dict.fromkeys(all_grades))

    print(f"  → 시트 {len(sheets)}개 | 이름 {len(all_names)}개 | 학과 {len(all_deps)}개 | 학년 {all_grades}")
    if all_names:
        print(f"     이름 샘플: {all_names[:6]}")
    if desc:
        print(f"     설명 추출: {desc[:60]}...")

    return _build_cases(fname, all_names, all_deps, all_grades, meta, description=desc)


# ---------------------------------------------------------------------------
# 크로스 도큐먼트 케이스
# ---------------------------------------------------------------------------
def build_cross_cases(files: list[Path]) -> list[dict]:
    fnames = [p.name for p in files]
    cases  = []

    cross = [
        # ── sql_금액 ──────────────────────────────────────────────
        {
            "question": "올해 지급된 장학금 총액은 얼마야",
            "category": "sql_금액", "expected_route": "sql",
            "ground_truth_keywords": ["1,670만원", "1670만원"],
            "ground_truth_note": "480+320+280+150+240+200=1670만원",
            "source_docs": fnames, "difficulty": "hard",
        },
        {
            "question": "가장 지급 금액이 큰 장학금은 뭐야",
            "category": "sql_금액", "expected_route": "sql",
            "ground_truth_keywords": ["신입생", "동문장학금", "480만원"],
            "ground_truth_note": "신입생 동문장학금 480만원으로 최다",
            "source_docs": fnames, "difficulty": "hard",
        },
        {
            "question": "성적우수 장학금 상반기랑 하반기 지급 금액 합계가 얼마야",
            "category": "sql_금액", "expected_route": "sql",
            "ground_truth_keywords": ["600만원", "320만원", "280만원"],
            "ground_truth_note": "320+280=600만원",
            "source_docs": ["성적우수 장학금 상반기 6월-320만원.pdf", "성적우수 장학금 하반기 12월-280만원.pdf"],
            "difficulty": "hard",
        },
        {
            "question": "9월에 지급된 장학금 총액은 얼마야",
            "category": "sql_금액", "expected_route": "sql",
            "ground_truth_keywords": ["390만원", "150만원", "240만원"],
            "ground_truth_note": "체육특기생 150만원 + 장학재단 특별 240만원 = 390만원",
            "source_docs": ["체육특기생 지원금 9월-150만원.xlsx", "장학재단 특별장학금 9월-240만원.pdf"],
            "difficulty": "hard",
        },
        {
            "question": "12월에 지급된 장학금 총액은 얼마야",
            "category": "sql_금액", "expected_route": "sql",
            "ground_truth_keywords": ["480만원", "280만원", "200만원"],
            "ground_truth_note": "성적우수 하반기 280만원 + 학년말 200만원 = 480만원",
            "source_docs": ["성적우수 장학금 하반기 12월-280만원.pdf", "학년말 성적우수 장학금 12월-200만원.xlsx"],
            "difficulty": "hard",
        },
        # ── sql_인원 ──────────────────────────────────────────────
        {
            "question": "올해 장학금 받은 학생 총 몇 명이야",
            "category": "sql_인원", "expected_route": "sql",
            "ground_truth_keywords": ["명"],
            "ground_truth_note": "전체 파일 합산 인원",
            "source_docs": fnames, "difficulty": "hard",
        },
        {
            "question": "성적우수 장학금 상반기랑 하반기 인원 비교해줘",
            "category": "sql_인원", "expected_route": "sql",
            "ground_truth_keywords": ["16명", "14명"],
            "ground_truth_note": "상반기 16명 vs 하반기 14명",
            "source_docs": ["성적우수 장학금 상반기 6월-320만원.pdf", "성적우수 장학금 하반기 12월-280만원.pdf"],
            "difficulty": "hard",
        },
        {
            "question": "인원이 가장 많은 장학금은 뭐야",
            "category": "sql_인원", "expected_route": "sql",
            "ground_truth_keywords": ["신입생", "동문장학금", "24명"],
            "ground_truth_note": "신입생 동문장학금 24명으로 최다",
            "source_docs": fnames, "difficulty": "hard",
        },
        # ── sql_명단 ──────────────────────────────────────────────
        {
            "question": "성적우수 장학금 받은 학생 전체 명단 알려줘",
            "category": "sql_명단", "expected_route": "sql",
            "ground_truth_keywords": ["성적우수"],
            "ground_truth_note": "상반기+하반기 통합 명단",
            "source_docs": ["성적우수 장학금 상반기 6월-320만원.pdf", "성적우수 장학금 하반기 12월-280만원.pdf"],
            "difficulty": "hard",
        },
        # ── vector_문서 ───────────────────────────────────────────
        {
            "question": "9월에 지급된 장학금 종류랑 목적 알려줘",
            "category": "vector_문서", "expected_route": "vector",
            "ground_truth_keywords": ["체육특기생", "장학재단", "체육 활동", "경제적"],
            "ground_truth_note": "체육특기생 + 장학재단 특별 목적 설명",
            "source_docs": ["체육특기생 지원금 9월-150만원.xlsx", "장학재단 특별장학금 9월-240만원.pdf"],
            "difficulty": "hard",
        },
        {
            "question": "한빛장학재단에서 지급한 장학금 설명해줘",
            "category": "vector_문서", "expected_route": "vector",
            "ground_truth_keywords": ["한빛장학재단", "특별장학금", "경제적"],
            "ground_truth_note": "장학재단 특별장학금 문서 설명",
            "source_docs": ["장학재단 특별장학금 9월-240만원.pdf"],
            "difficulty": "easy",
        },
        {
            "question": "동문회에서 지급한 장학금 설명해줘",
            "category": "vector_문서", "expected_route": "vector",
            "ground_truth_keywords": ["동문회", "신입생", "균등"],
            "ground_truth_note": "신입생 동문장학금 문서 설명",
            "source_docs": ["신입생 동문장학금 3월-480만원.xlsx"],
            "difficulty": "easy",
        },
        {
            "question": "체육특기생 지원금과 장학재단 특별장학금 목적 비교해줘",
            "category": "vector_문서", "expected_route": "vector",
            "ground_truth_keywords": ["체육 활동", "경제적 어려움"],
            "ground_truth_note": "두 문서의 지급 목적 비교",
            "source_docs": ["체육특기생 지원금 9월-150만원.xlsx", "장학재단 특별장학금 9월-240만원.pdf"],
            "difficulty": "hard",
        },
        {
            "question": "장학금 종류가 몇 가지야",
            "category": "vector_문서", "expected_route": "vector",
            "ground_truth_keywords": ["6", "종류"],
            "ground_truth_note": "총 6종류 장학금",
            "source_docs": fnames, "difficulty": "medium",
        },
    ]

    for c in cross:
        cases.append({"id": _next_id(), **c})

    return cases


# ---------------------------------------------------------------------------
# 배치 처리
# ---------------------------------------------------------------------------
def process_batch(batch: list[Path], skip_existing: set[str]) -> list[dict]:
    new_cases: list[dict] = []
    for path in batch:
        if path.name in skip_existing:
            print(f"  [SKIP] 건너뜀(이미 존재): {path.name}")
            continue
        print(f"\n  [FILE] 처리 중: {path.name}")
        try:
            if path.suffix.lower() not in _PARSEABLE_EXTS:
                print(f"     [SKIP] 파서 미지원 확장자: {path.suffix}")
                continue
            if path.suffix.lower() in (".xlsx", ".xls"):
                cases = cases_from_xlsx(path)
            else:
                cases = cases_from_pdf(path)
            print(f"     생성된 케이스: {len(cases)}개")
            new_cases.extend(cases)
        except Exception as e:
            print(f"     오류: {e}")
    return new_cases


# ---------------------------------------------------------------------------
# 메인
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="골드셋 자동 생성 (~100케이스)")
    parser.add_argument("--batch-size", type=int, default=3)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-existing", action="store_true")
    args = parser.parse_args()

    if GOLDSET_PATH.exists():
        goldset = json.loads(GOLDSET_PATH.read_text(encoding="utf-8"))
    else:
        goldset = {
            "version": "1.0",
            "description": "하이브리드 RAG 시스템 평가용 골드셋 — 데모 데이터 기반",
            "created": "2026-06-06",
            "source_docs": [],
            "test_cases": [],
        }

    existing_ids = {tc["id"] for tc in goldset["test_cases"]}
    if existing_ids:
        last_num = max(int(re.sub(r"\D", "", tid)) for tid in existing_ids)
        TC_ID_COUNTER[0] = last_num

    existing_sources: set[str] = set()
    if args.skip_existing:
        for tc in goldset["test_cases"]:
            existing_sources.update(tc.get("source_docs", []))

    files = find_data_files()
    if not files:
        print(f"'{DATA_DIR}'에서 PDF/XLSX 파일을 찾지 못했습니다.")
        sys.exit(1)

    print(f"총 {len(files)}개 파일 발견 (배치 크기={args.batch_size})")
    print("=" * 60)

    all_new_cases: list[dict] = []
    for i in range(0, len(files), args.batch_size):
        batch = files[i: i + args.batch_size]
        print(f"\n[배치 {i // args.batch_size + 1}] {[p.name for p in batch]}")
        new = process_batch(batch, existing_sources)
        all_new_cases.extend(new)
        print(f"  배치 소계: {len(new)}개")

    # 크로스 도큐먼트
    cross = build_cross_cases(files)
    all_new_cases.extend(cross)
    print(f"\n  [크로스 도큐먼트] {len(cross)}개 케이스 추가")

    print("\n" + "=" * 60)
    print(f"전체 신규 케이스: {len(all_new_cases)}개")

    if args.dry_run:
        print("\n[dry-run] goldset.json 저장 안 함.")
        print(f"케이스 분포:")
        from collections import Counter
        cat_cnt = Counter(c["category"] for c in all_new_cases)
        diff_cnt = Counter(c["difficulty"] for c in all_new_cases)
        for cat, cnt in sorted(cat_cnt.items()):
            print(f"  {cat}: {cnt}개")
        print(f"난이도: {dict(diff_cnt)}")
        return

    goldset["test_cases"].extend(all_new_cases)
    for p in files:
        if p.name not in goldset.get("source_docs", []):
            goldset.setdefault("source_docs", []).append(p.name)

    GOLDSET_PATH.write_text(
        json.dumps(goldset, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"저장 완료: {GOLDSET_PATH}")
    print(f"총 테스트케이스: {len(goldset['test_cases'])}개")


if __name__ == "__main__":
    main()
