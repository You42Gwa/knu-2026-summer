from __future__ import annotations

import logging
import time

from datastore.state import (
    _df_namespace, _df_sources, _df_labels,
    _df_schema_cache, _SCHEMA_CACHE_TTL,
)
import datastore.state as _state

logger = logging.getLogger("uvicorn.error")

_ENUM_KEYWORDS = ("학과", "학년", "종목", "계열", "반", "구분", "유형", "과목", "대상")


def _build_schema_for_vars(var_set: set[str]) -> str:
    """지정된 alias 집합에 대해서만 schema 문자열을 생성한다."""
    source_to_vars: dict[str, list[str]] = {}
    for var in var_set:
        src = _df_sources.get(var, var)
        source_to_vars.setdefault(src, []).append(var)

    parts: list[str] = []
    for src, vars_list in sorted(source_to_vars.items()):
        entry_lines = [f"파일: {src}"]
        for var in sorted(vars_list):
            df = _df_namespace[var]
            cols = list(df.columns)
            label = _df_labels.get(var, var)

            sample_str = ""
            if not df.empty:
                row = df.iloc[0]
                sample_str = ", ".join(
                    f"{c}={repr(str(v)[:20])}"
                    for c, v in row.items()
                    if v is not None and str(v) not in ("None", "nan", "")
                )[:200]

            quoted_cols = ", ".join(f'"{c}"' for c in cols)
            entry_lines.append(
                f"  데이터프레임: {var}  ({len(df)}행)  레이블: {label}\n"
                f"  컬럼(이 이름만 사용): {quoted_cols}\n"
                f"  예시(값): {sample_str}"
            )

            for col in cols:
                if any(k in col for k in _ENUM_KEYWORDS):
                    try:
                        uniq = df[col].dropna().unique()
                        if 0 < len(uniq) <= 20:
                            entry_lines.append(
                                f'  컬럼"{col}"의 실제값: {", ".join(str(v) for v in sorted(uniq)[:15])}'
                            )
                    except Exception:
                        pass

        parts.append("\n".join(entry_lines))
    return "\n\n".join(parts)


def _get_df_schema() -> str:
    """전체 schema (캐시됨)."""
    now = time.time()
    if _state._df_schema_cache and now - _state._df_schema_cache[1] < _SCHEMA_CACHE_TTL:
        return _state._df_schema_cache[0]
    schema = _build_schema_for_vars(set(_df_namespace.keys()))
    _state._df_schema_cache = (schema, now)
    return schema


def _get_df_schema_filtered(question: str) -> str:
    """질문과 관련된 DF만 포함한 schema를 반환한다.
    셀값 매칭 → 소스명 매칭 순으로 후보를 모으고, 없으면 전체 schema를 반환한다."""
    # 지연 임포트로 단방향 의존성 유지 (schema → query)
    from datastore.query import _find_filter_conditions, _find_dfs_by_source_label

    relevant: set[str] = set()

    # 셀값 매칭
    conditions = _find_filter_conditions(question)
    relevant.update(conditions.keys())

    # 소스명 매칭
    by_label = _find_dfs_by_source_label(question)
    relevant.update(by_label[:4])  # 상위 4개까지

    if not relevant:
        return _get_df_schema()

    # 관련 DF만 schema 생성
    schema = _build_schema_for_vars(relevant)
    logger.info("[SCHEMA_FILTER] %d/%d DFs 선택 | question=%s", len(relevant), len(_df_namespace), question[:40])
    return schema
