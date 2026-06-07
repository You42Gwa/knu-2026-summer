from __future__ import annotations

import logging
import re

import pandas as pd

from datastore.state import _df_namespace, _df_sources, _df_labels
from rag.router import _AGG_COUNT, _AGG_SUM, _AGG_MAX, _AGG_MIN, _AGG_PER

logger = logging.getLogger("uvicorn.error")

# ---------------------------------------------------------------------------
# 이름 검색용 상수
# ---------------------------------------------------------------------------
_NAME_COLS     = ("성명", "이름", "학생명", "수혜자명", "학생이름", "수혜자", "명단", "학생", "이_름", "성_명")
_NAME_COLS_SET = frozenset(_NAME_COLS)
_AMOUNT_COLS = ("금액", "지급액", "장학금액", "수혜금액", "지원금액", "장학금")

_NON_NAME_WORDS = frozenset([
    "장학금", "장학", "전기과", "건축과", "기계과", "화학과", "컴퓨터",
    "학과", "학년", "학생", "신입생", "재학생", "대상자", "수혜자",
    "성적", "우수자", "금액", "명단", "목록", "정보", "대학교",
    "이상", "이하", "미만", "해당", "지급", "기준", "선발",
    "알려줘", "알려주", "주세요", "해줘", "계열", "바이오", "화학",
    "동문장학", "동문회", "실습품", "확인서", "기능대회", "지원금",
    "공무원", "검도부", "관악부", "운동부", "축구부",
    "학년말", "성적우수", "총인원", "총금액", "얼마야", "얼마",
    "수령자", "수령확인", "출전선수", "학교운동", "스마트공간",
])

_KR_PARTICLES = frozenset("의이가을를은는에도로과와며서")

# source-label 검색용 제외 단어
_SOURCE_STOP_WORDS = frozenset([
    "학생", "이름", "알려줘", "알려주", "주세요", "해줘", "누구", "누구야",
    "몇명", "인원", "총인원", "총금액", "얼마야", "얼마",
])

_AMOUNT_IN_FILENAME_RE = re.compile(r"(\d[\d,]*)만원")
_MONTH_IN_FILENAME_RE  = re.compile(r"(\d{1,2})월")


def _strip_kr_particle(word: str) -> str:
    if len(word) >= 3 and word[-1] in _KR_PARTICLES:
        return word[:-1]
    return word


def _count_valid_name_rows(df: pd.DataFrame) -> int:
    """이름 컬럼이 있으면 비어있지 않은 행만 카운트, 없으면 전체 행 수."""
    name_col = next((c for c in df.columns if c in _NAME_COLS_SET), None)
    if name_col:
        valid = df[name_col].astype(str).str.strip()
        cnt = int((~valid.isin(["", "None", "nan", "NaN"])).sum())
        return cnt if cnt > 0 else len(df)
    return len(df)


def _expand_명단_column(df: pd.DataFrame) -> pd.DataFrame:
    """df2/df3처럼 '명단' 컬럼에 이름이 뭉쳐 있는 경우 행을 개별 이름으로 분리한다.

    원본 형식: "1반 22번 최성욱 2반 22번 추승민 ..."
    결과: 학과·성명·생년월일 컬럼으로 펼쳐진 DataFrame
    """
    if '명단' not in df.columns:
        return df
    rows: list[dict] = []
    for _, row in df.iterrows():
        명단_text  = str(row.get('명단', ''))
        생년월일_text = str(row.get('생년월일', ''))
        names     = re.findall(r'\d+반\s*\d+번\s*([가-힣]{2,4})', 명단_text)
        birthdates = re.findall(r'\d{6}', 생년월일_text)
        if names:
            for i, name in enumerate(names):
                rows.append({
                    '학과':   str(row.get('학과', '')),
                    '성명':   name,
                    '생년월일': birthdates[i] if i < len(birthdates) else '',
                })
        else:
            rows.append(row.to_dict())
    return pd.DataFrame(rows) if rows else df


def _search_name_pandas(question: str) -> tuple[pd.DataFrame | None, list[str], bool]:
    """질문에서 이름 후보를 추출해 모든 DataFrame에서 전수 검색."""
    seen: set[str] = set()
    candidates: list[str] = []
    for w in re.findall(r"[가-힣]{2,4}", question):
        clean = _strip_kr_particle(w)
        if clean not in _NON_NAME_WORDS and clean not in seen:
            candidates.append(clean)
            seen.add(clean)
    if not candidates:
        return None, [], False  # 이름 후보 없음

    context_words = {w for w in re.findall(r"[가-힣]{2,}", question)} - _NON_NAME_WORDS
    table_results: list[tuple[pd.DataFrame, str, int]] = []

    for var_name, df in _df_namespace.items():
        name_col = next((c for c in df.columns if c in _NAME_COLS_SET), None)
        if name_col is None:
            continue

        amount_cols = [c for c in df.columns if any(k in c for k in _AMOUNT_COLS)]

        for cand in candidates:
            try:
                mask = df[name_col].astype(str).str.contains(cand, na=False)
            except Exception:
                continue
            rows = df[mask]
            if rows.empty:
                continue

            if amount_cols:
                valid = rows[amount_cols].apply(
                    lambda col: ~col.astype(str).isin(["", "0", "-", "없음", "None", "nan"])
                ).any(axis=1)
                rows = rows[valid]
            if rows.empty:
                continue

            row_text = " ".join(rows.astype(str).values.flatten())
            src = _df_sources.get(var_name, var_name)
            ctx_score = sum(1 for w in context_words if w in row_text)
            src_score = sum(1 for w in context_words if w in src)
            score = ctx_score + src_score
            table_results.append((rows, src, score))
            break

    if not table_results:
        return None, [], True  # 이름 후보는 있었으나 데이터에 없음

    table_results.sort(key=lambda x: x[2], reverse=True)
    best_rows, best_src, best_score = table_results[0]
    logger.info("[NAME_SEARCH] %d개 DF 매칭, 최적 선택 (score=%d): %s",
                len(table_results), best_score, best_src)
    return best_rows, [best_src], True


def _find_filter_conditions(question: str) -> dict[str, list[tuple[str, str]]]:
    """질문 키워드를 실제 DataFrame 셀 값과 대조해 {alias: [(col, value), ...]} 반환."""
    if not _df_namespace:
        return {}

    candidates: list[str] = []
    seen: set[str] = set()
    for w in re.findall(r"[가-힣]{2,10}", question):
        stripped = _strip_kr_particle(w)
        for cand in dict.fromkeys([w, stripped]):
            if cand in seen or len(cand) < 2:
                continue
            # ~과 학과명은 NON_NAME_WORDS 제외 대상 (전기과, 친환경자동차과 등)
            if cand not in _NON_NAME_WORDS or (cand.endswith("과") and len(cand) >= 3):
                candidates.append(cand)
                seen.add(cand)

    for m in re.findall(r"20\d{2}|[1-4]학년", question):
        if m not in seen:
            candidates.append(m)
            seen.add(m)

    if not candidates:
        return {}

    result: dict[str, list[tuple[str, str]]] = {}
    visited: set[tuple[str, str]] = set()

    for cand in candidates[:10]:
        for alias, df in _df_namespace.items():
            for col in df.columns:
                if (alias, col) in visited:
                    continue
                try:
                    if df[col].astype(str).str.contains(re.escape(cand), na=False).any():
                        result.setdefault(alias, []).append((col, cand))
                        visited.add((alias, col))
                        break
                except Exception:
                    continue

    return result


def _find_dfs_by_source_label(question: str) -> list[str]:
    """데이터 셀 매칭이 없을 때 소스명·레이블을 키워드로 검색해 관련 alias 목록 반환."""
    words: set[str] = set()
    for w in re.findall(r"[가-힣]{2,}|20\d{2}|\d+월|\d+분기", question):
        stripped = _strip_kr_particle(w)
        # 조사를 모두 제거한 어근까지 추가 ("상반기에서" → "상반기")
        fully = w
        while len(fully) >= 3 and fully[-1] in _KR_PARTICLES:
            fully = fully[:-1]
        for cand in dict.fromkeys([w, stripped, fully]):
            if cand not in _SOURCE_STOP_WORDS and len(cand) >= 2:
                words.add(cand)

    scored: list[tuple[str, int]] = []
    for alias in _df_namespace:
        text = (_df_sources.get(alias, "") + " " + _df_labels.get(alias, ""))
        score = sum(1 for w in words if w in text)
        if score > 0:
            scored.append((alias, score))

    scored.sort(key=lambda x: x[1], reverse=True)
    return [a for a, _ in scored]


def _find_value_locations(question: str) -> str:
    """_find_filter_conditions 결과를 LLM 프롬프트용 힌트 문자열로 변환."""
    conditions = _find_filter_conditions(question)
    if not conditions:
        return ""
    hints = [
        f"'{val}' → {alias}['{col}'] (파일: {_df_sources.get(alias, alias)})"
        for alias, cond_list in conditions.items()
        for col, val in cond_list
    ]
    return "데이터 위치 힌트 (질문 맥락에 맞는 DataFrame을 선택하세요):\n" + "\n".join(
        f"  {h}" for h in hints
    )


def _extract_total_from_source(alias: str) -> str | None:
    """소스 파일명에서 총액 정보 추출 (예: '-760만원.pdf' → '760만원')."""
    src = _df_sources.get(alias, "")
    m = _AMOUNT_IN_FILENAME_RE.search(src)
    return f"{m.group(1)}만원" if m else None


def _extract_month_from_source(source: str) -> str:
    """파일명에서 지출월 추출 (예: '3월' → '3월')."""
    m = _MONTH_IN_FILENAME_RE.search(source)
    return f"{m.group(1)}월" if m else ""


def _extract_recipient_from_dfs(aliases: list[str]) -> str:
    """DataFrame의 '지급처' 컬럼에서 대표 지급처명 추출."""
    for alias in aliases:
        df = _df_namespace.get(alias)
        if df is None:
            continue
        col = next((c for c in df.columns if "지급처" in c), None)
        if col:
            vals = df[col].dropna()
            vals = vals[vals.astype(str).str.strip().ne("")]
            if not vals.empty:
                return str(vals.iloc[0]).strip()
    return ""


def _query_pandas_direct(question: str) -> tuple[object, list[str]]:
    """LLM 코드 생성 없이 키워드 매핑으로 직접 pandas 조회."""
    conditions = _find_filter_conditions(question)
    year_in_q = re.search(r"20\d{2}", question)
    year_str   = year_in_q.group() if year_in_q else None

    def _extract_year_from_alias(alias: str) -> int:
        src = _df_sources.get(alias, "") + _df_labels.get(alias, "")
        years = re.findall(r"20(\d{2})", src)
        return max(int(y) for y in years) if years else 0

    _src_keywords = set(re.findall(r"[가-힣]{2,}", question))

    def _src_relevance(alias: str) -> int:
        src = _df_sources.get(alias, "") + " " + _df_labels.get(alias, "")
        return sum(1 for w in _src_keywords if w in src)

    def _pick_best_alias(aliases: list[str]) -> str:
        """1순위: 소스명 키워드 유사도, 2순위: 연도(질문 연도 → 최신), 3순위: 조건 수."""
        if year_str:
            year_matched = [
                a for a in aliases
                if year_str in (_df_sources.get(a, "") + _df_labels.get(a, ""))
            ]
            if year_matched:
                return max(year_matched, key=lambda a: (_src_relevance(a), len(conditions.get(a, []))))

        def _score(a: str) -> tuple[int, int, int]:
            return (_src_relevance(a), _extract_year_from_alias(a), len(conditions.get(a, [])))

        return max(aliases, key=_score)

    grade_m = re.search(r"([1-4])학년", question)

    def _apply_grade_filter(df: pd.DataFrame) -> pd.DataFrame:
        if not grade_m:
            return df
        grade_col = next((c for c in df.columns if "학년" in c), None)
        if grade_col:
            try:
                return df[df[grade_col].astype(str).str.contains(grade_m.group(1), na=False)]
            except Exception:
                pass
        return df

    if conditions:
        best_alias = _pick_best_alias(list(conditions.keys()))
        df = _df_namespace[best_alias]

        mask = pd.Series([True] * len(df), index=df.index)
        for col, val in conditions[best_alias]:
            mask &= df[col].astype(str).str.contains(re.escape(val), na=False)
        filtered = _apply_grade_filter(df[mask])

    else:
        # 소스명 기반 fallback
        src_aliases = _find_dfs_by_source_label(question)
        if not src_aliases:
            # 학년 집계 전용 경로: "N학년 몇 명" 이면 전체 DF에서 학년 카운트
            if grade_m and _AGG_COUNT.search(question):
                total = 0
                sources: list[str] = []
                for alias, df in _df_namespace.items():
                    grade_col = next((c for c in df.columns if "학년" in c), None)
                    if grade_col:
                        try:
                            cnt = int(df[df[grade_col].astype(str) == grade_m.group(1)].shape[0])
                            if cnt > 0:
                                total += cnt
                                sources.append(_df_sources.get(alias, alias))
                        except Exception:
                            pass
                return (int(total), sources) if total > 0 else (None, [])
            return None, []

        best_alias = _pick_best_alias(src_aliases) if year_str else src_aliases[0]
        df = _df_namespace[best_alias]
        filtered = _apply_grade_filter(df)

    if filtered.empty:
        return None, []

    source = _df_sources.get(best_alias, best_alias)

    # 금액 컬럼 공통 탐색
    def _find_amount_col(df: pd.DataFrame):
        return next((c for c in df.columns if any(k in c for k in _AMOUNT_COLS)), None)

    def _to_numeric_clean(series: "pd.Series") -> "pd.Series":
        """콤마 포함 문자열('250,000')도 숫자로 변환."""
        return pd.to_numeric(series.astype(str).str.replace(",", "", regex=False), errors="coerce")

    # ── 최고/최저 금액 ──────────────────────────────────────────────────────
    if _AGG_MAX.search(question):
        amount_col = _find_amount_col(filtered)
        if amount_col:
            try:
                val = _to_numeric_clean(filtered[amount_col]).max()
                if not pd.isna(val):
                    return float(val), [source]
            except Exception:
                pass

    if _AGG_MIN.search(question):
        amount_col = _find_amount_col(filtered)
        if amount_col:
            try:
                val = _to_numeric_clean(filtered[amount_col]).min()
                if not pd.isna(val):
                    return float(val), [source]
            except Exception:
                pass

    # ── 1인당 지급액 ────────────────────────────────────────────────────────
    if _AGG_PER.search(question):
        amount_col = _find_amount_col(filtered)
        if amount_col:
            try:
                unique_vals = _to_numeric_clean(filtered[amount_col]).dropna().unique()
                if len(unique_vals) == 1:
                    return float(unique_vals[0]), [source]
                if len(unique_vals) > 1:
                    mode_val = _to_numeric_clean(filtered[amount_col]).mode()
                    if not mode_val.empty:
                        return float(mode_val.iloc[0]), [source]
            except Exception:
                pass

    # ── 특정 금액 기준 인원수 ───────────────────────────────────────────────
    amount_filter_m = re.search(r"(\d[\d,]+)원", question)
    if amount_filter_m and _AGG_COUNT.search(question):
        target = amount_filter_m.group(1).replace(",", "")
        amount_col = _find_amount_col(filtered)
        if amount_col:
            try:
                num_series = _to_numeric_clean(filtered[amount_col])
                count = int((num_series == float(target)).sum())
                return count, [source]
            except Exception:
                pass

    if _AGG_COUNT.search(question):
        # 동일 소스 파일에서 나온 여러 DF가 있으면 전체 합산
        same_src = [a for a in _df_namespace if _df_sources.get(a) == source]
        if len(same_src) > 1:
            def _count_for_alias(a: str) -> int:
                df_a = _df_namespace[a]
                m = pd.Series([True] * len(df_a), index=df_a.index)
                for col, val in conditions.get(best_alias, []):
                    if col in df_a.columns:
                        m &= df_a[col].astype(str).str.contains(re.escape(val), na=False)
                return _count_valid_name_rows(_apply_grade_filter(df_a[m]))
            total = sum(_count_for_alias(a) for a in same_src)
            logger.info("[AGG_COUNT] 동일 소스 %d개 DF 합산 | source=%s total=%d", len(same_src), source, total)
            return int(total), [source]
        return _count_valid_name_rows(filtered), [source]

    if _AGG_SUM.search(question):
        # 1인당 질문이 아닐 때만 총액 추출
        total_str = _extract_total_from_source(best_alias)
        if total_str:
            return total_str, [source]
        amount_col = _find_amount_col(filtered)
        if amount_col:
            try:
                total = pd.to_numeric(filtered[amount_col], errors="coerce").sum()
                return float(total), [source]
            except Exception:
                pass

    # 명단 컬럼이 있으면 개별 이름 행으로 변환
    return _expand_명단_column(filtered), [source]
