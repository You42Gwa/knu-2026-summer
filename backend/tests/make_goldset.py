"""
골드셋 자동 생성 스크립트

data/ 폴더의 PDF·XLSX 파일을 3개씩 읽어 테이블 데이터를 추출하고
goldset.json 에 test_case를 추가한다.

사용법:
    python make_goldset.py                  # 전체 파일 처리
    python make_goldset.py --batch-size 3   # 배치 크기 조정 (기본 3)
    python make_goldset.py --dry-run        # goldset.json 미저장, 콘솔 출력만
    python make_goldset.py --skip-existing  # 이미 source_docs에 있는 파일 건너뜀
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import pdfplumber

DATA_DIR     = Path(__file__).parent.parent / "data"
GOLDSET_PATH = Path(__file__).parent / "goldset.json"

# 파일명에서 번호 추출 → 정렬용
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
# 파일별 추출 유틸
# ---------------------------------------------------------------------------

def _clean(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() in ("nan", "none", "") else s


def read_xlsx(path: Path) -> list[dict]:
    """XLSX → [{sheet, df}]"""
    result = []
    xl = pd.ExcelFile(path, engine="openpyxl")
    for sheet in xl.sheet_names:
        df = xl.parse(sheet, header=None, dtype=str)
        df = df.fillna("").replace("nan", "")
        result.append({"sheet": sheet, "df": df})
    return result


def read_pdf_tables(path: Path) -> list[dict]:
    """PDF → [{page, table(list[list])}]"""
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
# 파일명에서 카테고리·금액 힌트 추출
# ---------------------------------------------------------------------------

def parse_filename_meta(name: str) -> dict:
    meta = {"amount": None, "label": name}
    m = re.search(r"(\d[\d,]+)만원", name)
    if m:
        meta["amount"] = m.group(0)
    return meta


# ---------------------------------------------------------------------------
# 파일 유형별 테이블 행 → 사람 이름 추출
# ---------------------------------------------------------------------------

# 이름이 아닌 한글 2~4자 단어 블랙리스트 (헤더, 학과명 단편, 단위어 등)
_NAME_BLACKLIST = {
    # 테이블 헤더
    "연번", "학과", "성명", "학년", "번호", "비고", "계열", "파트",
    "대상", "학생", "생년", "월일", "연락", "합계", "기계", "전기",
    "화학", "자동차", "건축", "건설", "생산", "공간", "순번", "이름",
    "반", "명", "부서", "직급", "소속", "구분", "항목", "내용",
    "금액", "지급", "수령", "확인", "서명", "날짜", "담당", "승인",
    "학교", "교장", "교감", "선생", "교사", "강사", "입학", "졸업",
    "국어", "영어", "수학", "과학", "체육", "음악", "미술", "기술",
    "종목", "선수", "코치", "감독", "부장", "위원", "회장", "총무",
    "성별", "남자", "여자", "남성", "여성", "관계", "보호", "부모",
    "형제", "자매", "직업", "주소", "우편", "전화", "이메일", "홈피",
    "기타", "참고", "메모", "특기", "사항", "결과", "점수", "성적",
    "등급", "순위", "석차", "통과", "합격", "불합격", "보류", "대기",
    # 4글자 헤더
    "생년월일", "대상학생", "연락처", "명단", "목록", "리스트",
    "장학금", "동문회", "수혜자", "대상자", "지급액", "총액", "지원금",
    "학교명", "학급명", "담임명", "반번호", "학번", "출석번호",
    # 악기·종목명 (관악부 데이터)
    "클라리넷", "플륫", "트롬본", "튜바", "트럼펫", "오보에",
    "플루트", "호른", "색소폰", "타악기", "퍼커션",
    # 학과 약어 및 기타 비이름 2자 단어
    "화공", "자공", "기계", "건설", "전기", "건축", "섬유", "화학",
    "학반", "학번", "반번", "출결", "결석", "조퇴", "지각",
    "현재", "변경", "이전", "상태", "조건",
}

# 이름이 아닌 접미사 (학과, 중학교, 사무처, 계열 등)
_NON_NAME_SUFFIX = re.compile(r"(과|계열|반|중|처|실|실장|부|부장|위|원|회|관|소|원장|장|팀)$")


def _is_name(s: str) -> bool:
    """한글 2~4자이고 블랙리스트 및 비이름 접미사에 해당하지 않는 경우 이름 후보."""
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
    """'과' 또는 '계열' 로 끝나는 셀 → 학과."""
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


# ---------------------------------------------------------------------------
# 파일 하나 분석 → test_case 목록 생성
# ---------------------------------------------------------------------------

TC_ID_COUNTER = [0]   # 전역 카운터 (기존 goldset 마지막 ID 이후로 시작)


def _next_id() -> str:
    TC_ID_COUNTER[0] += 1
    return f"TC{TC_ID_COUNTER[0]:03d}"


def cases_from_pdf(path: Path) -> list[dict]:
    fname  = path.name
    meta   = parse_filename_meta(fname)
    tables = read_pdf_tables(path)
    text   = read_pdf_text(path)

    all_rows: list[list] = []
    for t in tables:
        all_rows.extend(t["table"])

    names = extract_names_from_rows(all_rows)
    deps  = extract_departments_from_rows(all_rows)

    print(f"  → 테이블 {len(tables)}개 | 이름 {len(names)}개 | 학과 {len(deps)}개")
    if names:
        print(f"     이름 샘플: {names[:8]}")
    if deps:
        print(f"     학과 샘플: {deps[:5]}")

    cases: list[dict] = []

    # ── 명단 케이스 ──────────────────────────────────────────────
    if names:
        sample = names[:5]
        cases.append({
            "id": _next_id(),
            "question": f"{_doc_label(fname)} 명단을 알려줘",
            "category": "sql_명단",
            "expected_route": "sql",
            "ground_truth_keywords": sample,
            "ground_truth_note": f"'{fname}' 첫 5명 샘플",
            "source_docs": [fname],
            "difficulty": "easy",
        })

    # ── 학과별 명단 케이스 ────────────────────────────────────────
    if deps:
        dep = deps[0]
        dep_names = [
            n for n in names
            if any(
                _clean(cell) == dep and _is_name(_clean(row[j if j + 1 < len(row) else j]))
                for row in all_rows
                for j, cell in enumerate(row)
                if _clean(cell) == dep
            )
        ]
        kw = dep_names[:3] if dep_names else names[:2]
        cases.append({
            "id": _next_id(),
            "question": f"{dep} 명단 알려줘",
            "category": "sql_명단",
            "expected_route": "sql",
            "ground_truth_keywords": kw if kw else [dep],
            "ground_truth_note": f"'{fname}' {dep} 소속 명단",
            "source_docs": [fname],
            "difficulty": "easy",
        })

    # ── 인원 수 케이스 ───────────────────────────────────────────
    if names:
        cases.append({
            "id": _next_id(),
            "question": f"{_doc_label(fname)} 총 인원은 몇 명이야",
            "category": "sql_인원",
            "expected_route": "sql",
            "ground_truth_keywords": [f"{len(names)}명"],
            "ground_truth_note": f"추출된 이름 기준 총 {len(names)}명 (실제 중복·헤더 제외 검증 필요)",
            "source_docs": [fname],
            "difficulty": "easy",
        })

    # ── 금액 케이스 ──────────────────────────────────────────────
    if meta["amount"]:
        cases.append({
            "id": _next_id(),
            "question": f"{_doc_label(fname)} 지급 금액은 얼마야",
            "category": "vector_문서",
            "expected_route": "vector",
            "ground_truth_keywords": [meta["amount"]],
            "ground_truth_note": f"파일명 기준 총액 {meta['amount']}",
            "source_docs": [fname],
            "difficulty": "easy",
        })

    # ── 벡터 문서 케이스 ─────────────────────────────────────────
    if text.strip():
        cases.append({
            "id": _next_id(),
            "question": f"{_doc_label(fname)} 문서의 목적이나 내용을 설명해줘",
            "category": "vector_문서",
            "expected_route": "vector",
            "ground_truth_keywords": _guess_keywords(text, fname),
            "ground_truth_note": "문서 핵심 키워드 포함 여부 확인",
            "source_docs": [fname],
            "difficulty": "easy",
        })

    return cases


def cases_from_xlsx(path: Path) -> list[dict]:
    fname  = path.name
    meta   = parse_filename_meta(fname)
    sheets = read_xlsx(path)

    all_names: list[str] = []
    all_deps:  list[str] = []
    for s in sheets:
        all_names.extend(extract_names_from_df(s["df"]))
        all_deps.extend(extract_departments_from_df(s["df"]))

    # 중복 제거
    all_names = list(dict.fromkeys(all_names))
    all_deps  = list(dict.fromkeys(all_deps))

    print(f"  → 시트 {len(sheets)}개 | 이름 {len(all_names)}개 | 학과/부 {len(all_deps)}개")
    if all_names:
        print(f"     이름 샘플: {all_names[:8]}")
    if all_deps:
        print(f"     소속 샘플: {all_deps[:5]}")

    cases: list[dict] = []

    if all_names:
        sample = all_names[:5]
        cases.append({
            "id": _next_id(),
            "question": f"{_doc_label(fname)} 명단 알려줘",
            "category": "sql_명단",
            "expected_route": "sql",
            "ground_truth_keywords": sample,
            "ground_truth_note": f"'{fname}' 첫 5명 샘플",
            "source_docs": [fname],
            "difficulty": "easy",
        })

    if all_deps:
        dep = all_deps[0]
        cases.append({
            "id": _next_id(),
            "question": f"{dep} 소속 명단 알려줘",
            "category": "sql_명단",
            "expected_route": "sql",
            "ground_truth_keywords": [dep] + all_names[:2],
            "ground_truth_note": f"'{fname}' {dep} 소속 명단",
            "source_docs": [fname],
            "difficulty": "easy",
        })

    if all_names:
        cases.append({
            "id": _next_id(),
            "question": f"{_doc_label(fname)} 총 인원은 몇 명이야",
            "category": "sql_인원",
            "expected_route": "sql",
            "ground_truth_keywords": [f"{len(all_names)}명"],
            "ground_truth_note": f"추출 이름 기준 총 {len(all_names)}명 (검증 필요)",
            "source_docs": [fname],
            "difficulty": "easy",
        })

    if meta["amount"]:
        cases.append({
            "id": _next_id(),
            "question": f"{_doc_label(fname)} 지급 금액은 얼마야",
            "category": "vector_문서",
            "expected_route": "vector",
            "ground_truth_keywords": [meta["amount"]],
            "ground_truth_note": f"파일명 기준 총액 {meta['amount']}",
            "source_docs": [fname],
            "difficulty": "easy",
        })

    return cases


# ---------------------------------------------------------------------------
# 헬퍼
# ---------------------------------------------------------------------------

def _doc_label(fname: str) -> str:
    """파일명에서 자연스러운 문서 레이블 추출."""
    name = re.sub(r"^\d+\.\s*", "", fname)           # 앞 번호 제거
    name = re.sub(r"\.\w{2,4}$", "", name)            # 확장자 제거
    name = re.sub(r"\s*-?\s*\d[\d,]*만원", "", name)  # 금액 제거
    name = re.sub(r"\([^)]*\)", "", name).strip()      # 괄호+내용 전체 제거
    # 연도/학기/학교명 등 불필요 수식어 간략화
    name = re.sub(r"\d{4}학년도\s*", "", name)
    name = re.sub(r"\d{4}\.\s*", "", name)
    name = re.sub(r"(상반기|하반기)\s*", "", name)
    name = re.sub(r"대구공고\s*", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name if name else fname


def _guess_keywords(text: str, fname: str) -> list[str]:
    """문서 텍스트와 파일명에서 핵심 키워드 추출."""
    kw_patterns = [
        r"대구공고", r"장학금", r"관악부", r"검도부", r"축구부",
        r"신입생", r"성적우수", r"공무원", r"청솔", r"텍폴",
        r"기능대회", r"선수", r"대상자",
    ]
    found = []
    combined = fname + " " + text[:500]
    for pat in kw_patterns:
        if re.search(pat, combined):
            found.append(re.search(pat, combined).group(0))
    return found[:4] if found else ["대구공고"]


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
            if path.suffix.lower() in (".xlsx", ".xls"):
                cases = cases_from_xlsx(path)
            else:
                cases = cases_from_pdf(path)
            print(f"     생성된 케이스: {len(cases)}개")
            new_cases.extend(cases)
        except Exception as e:
            print(f"     ❌ 오류: {e}")
    return new_cases


# ---------------------------------------------------------------------------
# 메인
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="골드셋 자동 생성")
    parser.add_argument("--batch-size", type=int, default=3, help="배치 크기 (기본 3)")
    parser.add_argument("--dry-run", action="store_true", help="goldset.json 미저장")
    parser.add_argument("--skip-existing", action="store_true", help="이미 있는 파일 건너뜀")
    args = parser.parse_args()

    # 기존 골드셋 로드
    if GOLDSET_PATH.exists():
        goldset = json.loads(GOLDSET_PATH.read_text(encoding="utf-8"))
    else:
        goldset = {
            "version": "1.0",
            "description": "하이브리드 RAG 시스템 평가용 골드셋 — 실제 문서 기반",
            "created": "2026-05-07",
            "source_docs": [],
            "test_cases": [],
        }

    existing_ids = {tc["id"] for tc in goldset["test_cases"]}
    # 기존 마지막 TC 번호에서 이어서 시작
    if existing_ids:
        last_num = max(int(re.sub(r"\D", "", tid)) for tid in existing_ids)
        TC_ID_COUNTER[0] = last_num

    existing_sources: set[str] = set()
    if args.skip_existing:
        for tc in goldset["test_cases"]:
            existing_sources.update(tc.get("source_docs", []))

    files = find_data_files()
    if not files:
        print(f"'{DATA_DIR}' 에서 PDF/XLSX 파일을 찾지 못했습니다.")
        sys.exit(1)

    print(f"총 {len(files)}개 파일 발견 (배치 크기={args.batch_size})")
    print("=" * 60)

    all_new_cases: list[dict] = []
    for i in range(0, len(files), args.batch_size):
        batch = files[i : i + args.batch_size]
        batch_names = [p.name for p in batch]
        print(f"\n[배치 {i // args.batch_size + 1}] {batch_names}")
        new = process_batch(batch, existing_sources)
        all_new_cases.extend(new)
        print(f"  배치 소계: {len(new)}개 케이스 생성")

    print("\n" + "=" * 60)
    print(f"전체 신규 케이스: {len(all_new_cases)}개")

    if args.dry_run:
        print("\n[dry-run] goldset.json 저장 안 함. 미리보기:")
        print(json.dumps(all_new_cases, ensure_ascii=False, indent=2))
        return

    # 기존 goldset에 병합
    goldset["test_cases"].extend(all_new_cases)
    new_sources = [p.name for p in files]
    for s in new_sources:
        if s not in goldset.get("source_docs", []):
            goldset.setdefault("source_docs", []).append(s)

    GOLDSET_PATH.write_text(
        json.dumps(goldset, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"저장 완료: {GOLDSET_PATH}")
    print(f"총 테스트케이스: {len(goldset['test_cases'])}개")


if __name__ == "__main__":
    main()
