from __future__ import annotations

import glob
import json
import os
import re
import sys
import time
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional
from urllib.request import urlopen
from urllib.error import URLError

import pandas as pd

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Security
from fastapi.responses import StreamingResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel

from langchain_ollama import OllamaLLM, OllamaEmbeddings
from langchain_chroma import Chroma
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough

import chromadb

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.ingest import process_file, ensure_manifest_table, DATAFRAME_DIR

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
OLLAMA_BASE_URL     = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL        = os.getenv("OLLAMA_MODEL", "gemma4:e4b")
EMBED_MODEL         = os.getenv("EMBED_MODEL", "qwen3-embedding:0.6b")
CHROMA_HOST         = os.getenv("CHROMA_HOST", "localhost")
CHROMA_PORT         = int(os.getenv("CHROMA_PORT", "8000"))
COLLECTION_NAME     = os.getenv("COLLECTION_NAME", "scholarship_rules")
DATA_FOLDER         = os.path.join(os.path.dirname(__file__), "data")
API_KEY             = os.getenv("API_KEY", "")
INGEST_ALLOWED_BASE = os.path.realpath(os.getenv("INGEST_ALLOWED_BASE", DATA_FOLDER))

# ---------------------------------------------------------------------------
# API Key 인증
# ---------------------------------------------------------------------------
_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def _verify_api_key(key: str = Security(_api_key_header)):
    if API_KEY and key != API_KEY:
        raise HTTPException(status_code=401, detail="유효하지 않은 API Key입니다.")

# ---------------------------------------------------------------------------
# 파일 경로 검증 (Path Traversal 방지)
# ---------------------------------------------------------------------------
def _validate_ingest_path(file_path: str) -> str:
    abs_path = os.path.realpath(file_path)
    if not (abs_path == INGEST_ALLOWED_BASE or abs_path.startswith(INGEST_ALLOWED_BASE + os.sep)):
        raise HTTPException(
            status_code=400,
            detail=f"허용된 디렉토리 외부 파일에는 접근할 수 없습니다. (허용: {INGEST_ALLOWED_BASE})",
        )
    return abs_path

logger = logging.getLogger("uvicorn.error")

# ---------------------------------------------------------------------------
# LangChain 싱글턴
# ---------------------------------------------------------------------------
_llm_rag:  Optional[OllamaLLM] = None   # 답변 생성용
_llm_code: Optional[OllamaLLM] = None   # pandas 코드 생성용 (결정론적)
_retriever = None
_rag_chain = None


def get_llm_rag() -> OllamaLLM:
    global _llm_rag
    if _llm_rag is None:
        _llm_rag = OllamaLLM(
            base_url=OLLAMA_BASE_URL,
            model=OLLAMA_MODEL,
            temperature=0.1,
            num_ctx=4096,
        )
    return _llm_rag


def get_llm_code() -> OllamaLLM:
    global _llm_code
    if _llm_code is None:
        _llm_code = OllamaLLM(
            base_url=OLLAMA_BASE_URL,
            model=OLLAMA_MODEL,
            temperature=0.0,
            num_ctx=8192,
        )
    return _llm_code


def get_retriever():
    global _retriever
    if _retriever is None:
        embeddings = OllamaEmbeddings(base_url=OLLAMA_BASE_URL, model=EMBED_MODEL)
        client = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
        vectorstore = Chroma(
            client=client,
            collection_name=COLLECTION_NAME,
            embedding_function=embeddings,
        )
        _retriever = vectorstore.as_retriever(
            search_type="mmr",
            search_kwargs={"k": 8, "fetch_k": 30, "lambda_mult": 0.6},
        )
    return _retriever


def _fmt_docs(docs) -> str:
    parts = []
    for d in docs:
        src  = d.metadata.get("source", "")
        page = d.metadata.get("page", "")
        label = f"[{src} p.{page}]" if page else f"[{src}]"
        parts.append(f"{label}\n{d.page_content}")
    return "\n\n".join(parts)


def get_rag_chain():
    global _rag_chain
    if _rag_chain is None:
        _rag_chain = (
            {"context": get_retriever() | _fmt_docs, "question": RunnablePassthrough()}
            | RAG_PROMPT
            | get_llm_rag()
            | StrOutputParser()
        )
    return _rag_chain

# ---------------------------------------------------------------------------
# DataFrame 저장소 (인메모리)
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


def _build_schema_for_vars(var_set: set[str]) -> str:
    """지정된 alias 집합에 대해서만 schema 문자열을 생성한다."""
    _ENUM_KEYWORDS = ("학과", "학년", "종목", "계열", "반", "구분", "유형", "과목", "대상")
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
    global _df_schema_cache
    now = time.time()
    if _df_schema_cache and now - _df_schema_cache[1] < _SCHEMA_CACHE_TTL:
        return _df_schema_cache[0]
    schema = _build_schema_for_vars(set(_df_namespace.keys()))
    _df_schema_cache = (schema, now)
    return schema


def _get_df_schema_filtered(question: str) -> str:
    """질문과 관련된 DF만 포함한 schema를 반환한다.
    셀값 매칭 → 소스명 매칭 순으로 후보를 모으고, 없으면 전체 schema를 반환한다."""
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

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------
MULTI_QUERY_PROMPT = PromptTemplate(
    input_variables=["question"],
    template="""\
사용자의 질문을 서로 다른 표현으로 3가지 재구성하세요. 한국어로 작성하고 한 줄에 하나씩 쓰세요.

원래 질문: {question}
재구성된 질문:""",
)

# 문서 설명 질문 탐지 (목적·내용·요약 요청)
_DOC_EXPLAIN_RE = re.compile(r"문서의?\s*(목적|내용|설명)|설명해|어떤\s*(문서|내용)|요약해", re.IGNORECASE)

_RAG_TEMPLATE = """\
당신은 한국어 문서를 분석하는 전문 AI 어시스턴트입니다.
아래 참고 문서를 바탕으로 질문에 정확하고 상세하게 한국어로 답변하세요.
참고 문서에 없는 내용은 "해당 내용은 문서에서 확인할 수 없습니다."라고 답하세요.

참고 문서:
{context}

질문: {question}
답변:"""

# 문서 설명 전용 템플릿: 문서명·금액·항목에서 목적·내용을 추론하도록 유도
_DOC_EXPLAIN_RAG_TEMPLATE = """\
당신은 한국어 문서를 분석하는 AI 어시스턴트입니다.
아래 참고 문서(특히 [문서 개요] 섹션)를 바탕으로 문서의 목적과 내용을 한국어로 설명하세요.
문서명, 지원 금액, 대상 기관·학생, 포함 항목 등 알 수 있는 정보를 모두 활용하세요.
참고 문서에 문서명이라도 있으면 그것을 근거로 설명해 주세요.

참고 문서:
{context}

질문: {question}
답변:"""

_PANDAS_GEN_TEMPLATE = """\
당신은 pandas 전문가입니다. 아래 스키마와 힌트를 보고 질문에 답하는 Python 코드를 작성하세요.
import 없이 변수명(df0, df1 ...)을 바로 사용하세요. 최종 결과는 반드시 result 변수에 저장하세요.
마크다운 코드 블록 없이 순수 Python 코드만 출력하세요.

★ 핵심 규칙:
- "데이터 위치 힌트"에 여러 옵션이 있으면 파일명을 보고 질문과 가장 관련 있는 DataFrame을 선택하세요.
- 힌트가 없을 때는 스키마의 파일명을 참고해 질문에 맞는 DataFrame을 직접 선택하세요.
- 컬럼명은 반드시 스키마의 "컬럼(이 이름만 사용):" 줄에서 가져오세요. 없는 컬럼명은 만들지 마세요.
- "실제값:" 줄은 데이터 값이지 컬럼명이 아닙니다.

코딩 규칙:
1. 텍스트 검색: df['컬럼명'].str.contains('값', na=False)
2. 인원수: result = int(len(filtered_df))
3. 금액 합계: result = float(df['컬럼명'].sum())
4. 명단 조회: result = filtered_df.to_dict('records')
5. 여러 DataFrame 합치기: pd.concat([df0, df1], ignore_index=True)
6. 대소문자 무시: str.contains('값', case=False, na=False)
7. 숫자 비교: df['컬럼명'].astype(float) >= 값

데이터프레임 스키마:
{schema}

{hints}
질문: {question}
코드:"""

_DATA_ANSWER_TEMPLATE = """\
아래 데이터 조회 결과를 바탕으로 질문에 한국어로만 자연스럽고 상세하게 답변하세요.
한자·일본어·중국어를 절대 섞지 마세요.
규칙:
- 조회 결과의 모든 항목을 빠짐없이 나열하세요.
- 이름·학과·학년·금액 등 관련 정보를 함께 제시하세요.
- 목록이 여러 명이면 번호를 붙여 나열하세요.
- 컬럼명(예: "성명", "학과")을 그대로 노출하지 말고 자연스러운 문장으로 표현하세요.
- 6자리 숫자가 생년월일·날짜 컬럼에 있으면 YYMMDD 형식으로 해석하세요.
  YY 00~30 → 2000년대(20YY년), YY 31~99 → 1900년대(19YY년).
- 연락처·주민번호 등 민감 개인정보는 질문에서 명시적으로 요청하지 않으면 생략하세요.
- COUNT 결과는 "총 X명" 또는 "X명"으로, SUM 결과는 금액에 따라 "X만원" 또는 "X원"으로 표현하세요.
- 금액이 원 단위(예: 2500000)이면 만원으로 환산하세요(예: 250만원). 소수점은 생략하세요.
- 데이터가 없으면 "조회된 데이터가 없습니다."라고 답하세요.
- 임의로 요약하거나 생략하지 마세요.

질문: {question}
조회 결과:
{result}
답변:"""

RAG_PROMPT = PromptTemplate.from_template(_RAG_TEMPLATE)

# ---------------------------------------------------------------------------
# 라우팅 (키워드 기반)
# ---------------------------------------------------------------------------
_PANDAS_KEYWORDS = re.compile(
    r"명단|몇\s*명|\d+\s*명|인원|금액|얼마|통계|집계|합계|총\s*금액|지급액|목록|리스트|누가|누구|현황|조회|어느\s*학과|무슨\s*학과|어느\s*반|종목",
    re.IGNORECASE,
)
_VECTOR_PROCEDURE = re.compile(
    r"방법|절차|기준|서류|자격|안내|규정|내용|제도|신청|문의|어떻게|왜|이유|달라|같아|차이|비교",
    re.IGNORECASE,
)
# "지급\s*금액" 제거: 파일명 기반 총액 추출로 pandas에서 처리
_VECTOR_OVERRIDE = re.compile(
    r"설명해|설명해줘|목적|문서의\s*내용|내용을\s*설명|어떤\s*내용|몇\s*월|몇\s*년|날짜|작성됐|어느\s*학교|총\s*지급액|장학금\s*총액",
    re.IGNORECASE,
)
_AGG_COUNT = re.compile(r"몇\s*명|총\s*인원|인원은|명이야|명인가|몇명", re.IGNORECASE)
_AGG_SUM   = re.compile(r"총\s*금액|합계금액|얼마야|얼마인|지급\s*금액|장학금\s*총액", re.IGNORECASE)


def _route(question: str) -> str:
    if _VECTOR_OVERRIDE.search(question):
        return "VECTOR"
    # _AGG_SUM은 PANDAS에서 소스 파일명 기반 총액 추출로 처리
    if _PANDAS_KEYWORDS.search(question) or _AGG_SUM.search(question):
        if _VECTOR_PROCEDURE.search(question):
            return "VECTOR"
        return "PANDAS"
    if "장학" in question and not _VECTOR_PROCEDURE.search(question):
        return "PANDAS"
    return "VECTOR"

# ---------------------------------------------------------------------------
# Vector RAG
# ---------------------------------------------------------------------------
_VECTOR_EMPTY_SIGNALS = ("해당 내용은 문서에서 확인할 수 없습니다", "문서에서 확인할 수 없")


async def _answer_vector(question: str, allow_pandas_fallback: bool = True) -> tuple[str, list[str], str]:
    logger.info("[VECTOR] 검색 시작 | question=%s", question[:50])

    queries = [question]
    is_doc_explain = bool(_DOC_EXPLAIN_RE.search(question))

    if is_doc_explain:
        # 문서 설명 질문: 메타 문구 제거 후 "[문서 개요]" 접두사로 개요 청크 검색
        doc_ctx = re.sub(r"\s*문서의?\s*(목적|내용|설명).*$", "", question).strip()
        doc_ctx = re.sub(r"\s*설명해.*$", "", doc_ctx).strip()
        if doc_ctx and len(doc_ctx) > 3:
            queries.insert(0, f"[문서 개요] {doc_ctx}")
        logger.info("[VECTOR] 문서설명 쿼리 최적화 | doc_ctx=%s", doc_ctx[:40])
    else:
        try:
            raw_variants = await get_llm_code().ainvoke(
                MULTI_QUERY_PROMPT.format(question=question)
            )
            variants = [l.strip() for l in raw_variants.strip().split("\n") if l.strip()]
            queries += variants[:2]
            logger.info("[VECTOR] 쿼리 확장 %d개", len(queries))
        except Exception as e:
            logger.warning("[VECTOR] 쿼리 확장 실패 | err=%s", e)

    retriever = get_retriever()
    all_docs: list = []
    seen: set[str] = set()
    for q in queries:
        try:
            for d in await retriever.ainvoke(q):
                key = d.page_content[:80]
                if key not in seen:
                    seen.add(key)
                    all_docs.append(d)
        except Exception:
            pass
    docs = all_docs[:12]

    source_files = list(dict.fromkeys(
        os.path.basename(d.metadata.get("source", ""))
        for d in docs if d.metadata.get("source")
    ))
    context = _fmt_docs(docs)

    # 문서 설명 질문은 전용 템플릿 사용 (목적·내용 추론 유도)
    if is_doc_explain:
        prompt = PromptTemplate.from_template(_DOC_EXPLAIN_RAG_TEMPLATE)
    else:
        prompt = RAG_PROMPT
    answer = await (prompt | get_llm_rag() | StrOutputParser()).ainvoke(
        {"context": context, "question": question}
    )
    logger.info("[VECTOR] 답변 생성 완료 | len=%d docs=%d", len(answer), len(docs))
    # 문서설명·목적 질문은 pandas 폴백 금지 (명단 테이블이 반환되면 더 나쁨)
    if allow_pandas_fallback and not _VECTOR_OVERRIDE.search(question) and any(s in answer for s in _VECTOR_EMPTY_SIGNALS):
        logger.info("[VECTOR→PANDAS] 유의미한 답변 없음, pandas 폴백 시도")
        pd_answer, pd_sources, _ = await _answer_pandas(question, allow_vector_fallback=False)
        if pd_answer and "없습니다" not in pd_answer and "오류" not in pd_answer:
            return pd_answer, pd_sources, "pandas"
    return answer, source_files, "vector"

# ---------------------------------------------------------------------------
# 이름 전수 검색 (pandas)
# ---------------------------------------------------------------------------
# 정확한 컬럼명 매칭용: any(k in c …) 대신 c in _NAME_COLS_SET 으로 사용
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
    "", "공무원", "", "검도부", "관악부", "운동부", "축구부",
    "학년말", "성적우수", "학교명", "총인원", "총금액", "얼마야", "얼마",
    "수령자", "수령확인", "출전선수", "학교운동", "스마트공간",
    # 학과명/종목명: _find_filter_conditions 에서 실제 셀값 검색에 사용되도록 제거
    # "자동화기계", "친환경자동차", "자동차정비", "자동차기계", "섬유소재", "전공심화", "공간건축"
])

_KR_PARTICLES = frozenset("의이가을를은는에도로과와며서")


def _strip_kr_particle(word: str) -> str:
    if len(word) >= 3 and word[-1] in _KR_PARTICLES:
        return word[:-1]
    return word


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

# ---------------------------------------------------------------------------
# 키워드 → 실제 (alias, col, value) 매핑
# ---------------------------------------------------------------------------
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


def _find_filter_conditions(question: str) -> dict[str, list[tuple[str, str]]]:
    """질문 키워드를 실제 DataFrame 셀 값과 대조해 {alias: [(col, value), ...]} 반환.

    - 긴 학과명(친환경자동차과 8자 등)을 잡기 위해 regex를 {2,10}으로 확장
    - 원본 단어를 stripped 버전보다 우선 시도
    - ~과 학과명은 NON_NAME_WORDS에 있어도 허용
    - 연도/학년 패턴 추가 추출
    """
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


# source-label 검색용 제외 단어 — NON_NAME_WORDS보다 훨씬 작은 집합
_SOURCE_STOP_WORDS = frozenset([
    "학생", "이름", "알려줘", "알려주", "주세요", "해줘", "누구", "누구야",
    "몇명", "인원", "총인원", "총금액", "얼마야", "얼마",
])


def _find_dfs_by_source_label(question: str) -> list[str]:
    """데이터 셀 매칭이 없을 때 소스명·레이블을 키워드로 검색해 관련 alias 목록 반환.
    _SOURCE_STOP_WORDS만 제거하므로 '동문회', '신입생' 같은 키워드도 살아남는다."""
    words: set[str] = set()
    # "3월", "1분기" 같은 숫자+한글 패턴도 추출해 파일명 내 월·분기 구분에 활용
    for w in re.findall(r"[가-힣]{2,}|20\d{2}|\d+월|\d+분기", question):
        stripped = _strip_kr_particle(w)
        for cand in [w, stripped]:
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
    """_find_filter_conditions 결과를 LLM 프롬프트용 힌트 문자열로 변환.
    소스 파일명을 함께 표시해 LLM이 질문 맥락에 맞는 DataFrame을 선택하도록 안내한다."""
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


_AMOUNT_IN_FILENAME_RE = re.compile(r"(\d[\d,]*)만원")


def _extract_total_from_source(alias: str) -> str | None:
    """소스 파일명에서 총액 정보 추출 (예: '-760만원.pdf' → '760만원')."""
    src = _df_sources.get(alias, "")
    m = _AMOUNT_IN_FILENAME_RE.search(src)
    return f"{m.group(1)}만원" if m else None


def _query_pandas_direct(question: str) -> tuple[object, list[str]]:
    """LLM 코드 생성 없이 키워드 매핑으로 직접 pandas 조회.

    처리 패턴:
    - 인원수 (몇 명, 총 인원)         → int(len(filtered))
    - 금액 합계 (총 금액, 얼마)        → float(amount_col.sum())
    - 명단/목록 (명단, 목록, 누가 …)   → filtered DataFrame

    데이터 셀에 매칭이 없으면 소스명으로 DataFrame을 선택한다.
    연도가 질문에 있으면 해당 연도 소스를 우선한다.
    """
    conditions = _find_filter_conditions(question)
    year_in_q = re.search(r"20\d{2}", question)
    year_str   = year_in_q.group() if year_in_q else None

    def _extract_year_from_alias(alias: str) -> int:
        src = _df_sources.get(alias, "") + _df_labels.get(alias, "")
        years = re.findall(r"20(\d{2})", src)
        return max(int(y) for y in years) if years else 0

    # 소스명 관련성 점수: 질문 키워드가 소스 파일명에 얼마나 등장하는지
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


    if _AGG_COUNT.search(question):
        # 동일 소스 파일에서 나온 여러 DF가 있으면 전체 합산 (예: 관악부 3개 테이블)
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
        # 1순위: 소스 파일명에서 총액 추출 (파일명에 "XYZ만원" 패턴)
        total_str = _extract_total_from_source(best_alias)
        if total_str:
            return total_str, [source]
        # 2순위: 금액 컬럼 합계
        amount_col = next(
            (c for c in filtered.columns if any(k in c for k in _AMOUNT_COLS)), None
        )
        if amount_col:
            try:
                total = pd.to_numeric(filtered[amount_col], errors="coerce").sum()
                return float(total), [source]
            except Exception:
                pass

    # 명단 컬럼이 있으면 개별 이름 행으로 변환
    return _expand_명단_column(filtered), [source]


# ---------------------------------------------------------------------------
# pandas 코드 생성 및 실행
# ---------------------------------------------------------------------------
_FORBIDDEN_CODE = re.compile(
    r'\b(import|exec|eval|compile|__import__|__builtins__|'
    r'getattr|setattr|delattr|globals|locals|vars|open|input)\b'
    r'|os\.|sys\.|subprocess\.|shutil\.|pathlib\.',
    re.IGNORECASE,
)

try:
    import numpy as np
    _EXEC_GLOBALS: dict = {"pd": pd, "np": np, "__builtins__": {
        "len": len, "str": str, "int": int, "float": float, "bool": bool,
        "list": list, "dict": dict, "tuple": tuple, "set": set,
        "sum": sum, "min": min, "max": max, "abs": abs, "round": round,
        "sorted": sorted, "enumerate": enumerate, "zip": zip,
        "range": range, "isinstance": isinstance,
        "True": True, "False": False, "None": None,
    }}
except ImportError:
    _EXEC_GLOBALS = {"pd": pd, "__builtins__": {
        "len": len, "str": str, "int": int, "float": float, "bool": bool,
        "list": list, "dict": dict, "tuple": tuple, "set": set,
        "sum": sum, "min": min, "max": max, "abs": abs, "round": round,
        "sorted": sorted, "enumerate": enumerate, "zip": zip,
        "range": range, "isinstance": isinstance,
        "True": True, "False": False, "None": None,
    }}


def _count_valid_name_rows(df: pd.DataFrame) -> int:
    """이름 컬럼이 있으면 비어있지 않은 행만 카운트, 없으면 전체 행 수."""
    name_col = next((c for c in df.columns if c in _NAME_COLS_SET), None)
    if name_col:
        valid = df[name_col].astype(str).str.strip()
        cnt = int((~valid.isin(["", "None", "nan", "NaN"])).sum())
        return cnt if cnt > 0 else len(df)
    return len(df)


def _clean_code(raw: str) -> str:
    code = re.sub(r"```(?:python)?", "", raw, flags=re.IGNORECASE).replace("```", "").strip()
    # 전각 특수문자(U+FF01-FF60) 및 전각 공백 제거
    code = re.sub(r"[！-｠　]", "", code)
    # LLM이 삽입하는 import / from … import 줄 제거
    code = re.sub(r"^(import|from)\s+\S.*$", "", code, flags=re.MULTILINE).strip()
    return code


def _exec_pandas_code(code: str) -> object:
    if _FORBIDDEN_CODE.search(code):
        raise ValueError("금지된 코드 패턴이 감지되었습니다.")
    namespace = dict(_EXEC_GLOBALS)
    namespace.update(_df_namespace)
    exec(code, namespace)
    return namespace.get("result")


def _format_pandas_result(result: object) -> str:
    if result is None:
        return "조회된 데이터가 없습니다."
    # numpy scalar
    if hasattr(result, "item"):
        result = result.item()
    if isinstance(result, (int, float)):
        return str(result)
    if isinstance(result, pd.Series):
        result = result.reset_index().to_dict("records")
    if isinstance(result, pd.DataFrame):
        if result.empty:
            return "조회된 데이터가 없습니다."
        return result.to_string(index=False)
    if isinstance(result, list):
        if not result:
            return "조회된 데이터가 없습니다."
        if isinstance(result[0], dict):
            cols = list(result[0].keys())
            lines = [" | ".join(cols), "-" * max(len(" | ".join(cols)), 1)]
            for row in result:
                lines.append(" | ".join(str(row.get(c, "-")) for c in cols))
            return "\n".join(lines)
        return "\n".join(str(r) for r in result)
    return str(result)


def _format_list_result(df: pd.DataFrame) -> str:
    """DataFrame 명단 결과를 LLM 우회로 직접 포맷.
    LLM에 넘기면 전체 행을 재생성하다 timeout 발생 → 테이블 그대로 반환."""
    if df is None or (hasattr(df, "empty") and df.empty):
        return "조회된 데이터가 없습니다."
    header = f"총 {len(df)}건\n"
    return header + df.to_string(index=False)


def _format_scalar_result(result: object, question: str) -> str:
    """int/float/str scalar를 LLM 없이 자연스러운 문장으로 포맷."""
    if hasattr(result, "item"):
        result = result.item()
    if isinstance(result, int):
        if _AGG_COUNT.search(question):
            return f"총 {result}명입니다."
        return str(result)
    if isinstance(result, float):
        if result == int(result):
            return _format_scalar_result(int(result), question)
        if int(result) >= 10000:
            return f"{int(result) // 10000}만원"
        return str(int(result))
    if isinstance(result, str):
        if re.search(r"\d+만원", result):
            return f"지급 금액은 {result}입니다."
        return result
    return str(result)

# ---------------------------------------------------------------------------
# Pandas RAG
# ---------------------------------------------------------------------------
_NO_VECTOR_FALLBACK = re.compile(r"누가|누구|명단|목록|리스트|몇\s*명|인원")


async def _answer_pandas(question: str, allow_vector_fallback: bool = True) -> tuple[str, list[str], str]:
    if not _df_namespace:
        return "현재 로드된 데이터프레임이 없습니다.", [], "pandas"

    # 1단계: 이름 전수 검색 (기존)
    name_df, name_sources, name_searched = _search_name_pandas(question)
    if name_df is not None:
        logger.info("[NAME_SEARCH] %d건 발견, 코드 생성 생략", len(name_df))
        return _format_list_result(name_df), name_sources, "pandas"
    if name_searched and re.search(r"이라는|라는\s*학생|학생이\s*(?:장학금|받|있)", question):
        # "홍길동이라는 학생이 장학금 받았어" 같은 특정 인물 조회 → 없음 반환
        logger.info("[NAME_SEARCH] 특정 인물 조회 패턴 — 데이터 없음")
        return "조회된 데이터가 없습니다.", [], "pandas"

    # 2단계: 키워드 직접 조회 (LLM 코드 생성 없음)
    direct_result, direct_sources = _query_pandas_direct(question)
    if direct_result is not None:
        formatted = _format_pandas_result(direct_result)
        if formatted != "조회된 데이터가 없습니다.":
            logger.info("[DIRECT] 직접 조회 성공 | source=%s", direct_sources)
            if isinstance(direct_result, pd.DataFrame):
                return _format_list_result(direct_result), direct_sources, "pandas"
            # scalar(int/float/str): LLM 우회, 직접 포맷
            return _format_scalar_result(direct_result, question), direct_sources, "pandas"

    # 3단계: LLM 코드 생성 (복잡한 질문 폴백) — 관련 DF만 schema에 포함
    schema = _get_df_schema_filtered(question)
    hints = _find_value_locations(question)
    agg_hint = ""
    if _AGG_COUNT.search(question):
        agg_hint = "\n※ 인원수 질문: result = int(len(filtered_df))"
    elif _AGG_SUM.search(question):
        agg_hint = "\n※ 금액 합계 질문: result = float(df['금액컬럼'].sum())"

    prompt_text = _PANDAS_GEN_TEMPLATE.format(schema=schema, hints=hints, question=question) + agg_hint

    logger.info("[PANDAS] 코드 생성 중 | question=%s", question[:50])
    raw_code = await get_llm_code().ainvoke(prompt_text)
    code = _clean_code(raw_code)
    logger.info("[PANDAS] 생성된 코드 | %s", code[:300])

    result = None
    code_err: str | None = None
    try:
        result = _exec_pandas_code(code)
    except Exception as e:
        code_err = str(e)
        logger.error("[PANDAS] 실행 오류 | err=%s | code=%s", e, code[:200])

    # 결과 없거나 오류 → 재시도
    is_empty = result is None or (hasattr(result, "__len__") and len(result) == 0)
    if is_empty or code_err:
        retry_ctx = f"\n이전 코드가 실패했거나 결과가 없었습니다.\n이전 코드:\n{code}"
        if code_err:
            retry_ctx += f"\n오류: {code_err}"
        retry_ctx += "\n조건을 완화(str.contains 사용)하거나 다른 데이터프레임을 사용하세요."

        raw_code2 = await get_llm_code().ainvoke(prompt_text + retry_ctx)
        code2 = _clean_code(raw_code2)
        if code2 and code2 != code:
            logger.info("[PANDAS] 재시도 코드 | %s", code2[:300])
            try:
                result = _exec_pandas_code(code2)
                code = code2
            except Exception as e2:
                logger.error("[PANDAS] 재시도 실패 | err=%s", e2)

    formatted = _format_pandas_result(result)

    if formatted == "조회된 데이터가 없습니다." and allow_vector_fallback:
        if _NO_VECTOR_FALLBACK.search(question):
            logger.info("[PANDAS] 명단형 쿼리 — VECTOR 폴백 건너뜀")
            return formatted, [], "pandas"
        logger.info("[PANDAS→VECTOR] 결과 없음, VECTOR 폴백")
        v_answer, v_sources, _ = await _answer_vector(question, allow_pandas_fallback=False)
        return v_answer, v_sources, "vector"

    source_files = list({_df_sources.get(v, v) for v in _df_namespace if v in code})

    if formatted == "조회된 데이터가 없습니다.":
        return formatted, source_files, "pandas"

    if isinstance(result, pd.DataFrame):
        return _format_list_result(result), source_files, "pandas"

    return _format_scalar_result(result, question), source_files, "pandas"

# ---------------------------------------------------------------------------
# 파일 탐색 (재귀)
# ---------------------------------------------------------------------------
def _find_files(folder: str) -> list[str]:
    paths = []
    for ext in ("xlsx", "pdf", "hwp"):
        paths.extend(glob.glob(os.path.join(folder, "**", f"*.{ext}"), recursive=True))
    return [p for p in paths if not os.path.basename(p).startswith(".")]

# ---------------------------------------------------------------------------
# 앱 수명 주기
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_manifest_table()
    logger.info("manifest 테이블 확인 완료")

    _load_dataframes()
    logger.info("DataFrame 로드 완료 | count=%d", len(_df_namespace))

    try:
        logger.info("LLM 워밍업 중... (model=%s)", OLLAMA_MODEL)
        await get_llm_rag().ainvoke("안녕")
        logger.info("LLM 워밍업 완료")
    except Exception as e:
        logger.warning("LLM 워밍업 실패 | model=%s err=%s", OLLAMA_MODEL, e)
    try:
        logger.info("임베딩 모델 워밍업 중... (model=%s)", EMBED_MODEL)
        OllamaEmbeddings(base_url=OLLAMA_BASE_URL, model=EMBED_MODEL).embed_query("안녕")
        logger.info("임베딩 워밍업 완료")
    except Exception as e:
        logger.warning("임베딩 워밍업 실패 | model=%s err=%s", EMBED_MODEL, e)

    yield

app = FastAPI(title="Local RAG Chatbot API", version="2.0.0", lifespan=lifespan)

# ---------------------------------------------------------------------------
# 스키마
# ---------------------------------------------------------------------------
class ChatRequest(BaseModel):
    question: str

class ChatResponse(BaseModel):
    answer: str
    source: str
    sources: list[str] = []

class IngestRequest(BaseModel):
    file_path: str

class StatusResponse(BaseModel):
    status: str
    message: str

# ---------------------------------------------------------------------------
# 엔드포인트
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    result: dict = {
        "status":      "ok",
        "llm_model":   OLLAMA_MODEL,
        "embed_model": EMBED_MODEL,
        "dataframes":  len(_df_namespace),
    }
    try:
        urlopen(f"{OLLAMA_BASE_URL}/api/tags", timeout=3)
        result["ollama"] = "ok"
    except URLError:
        result["ollama"] = "unreachable"
        result["status"] = "degraded"
    try:
        chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT).heartbeat()
        result["chromadb"] = "ok"
    except Exception:
        result["chromadb"] = "unreachable"
        result["status"] = "degraded"
    return result


@app.get("/summary")
def summary(_: None = Depends(_verify_api_key)):
    """모든 적재 문서의 명세 요약: 문서별 목적·인원·총액 + 전체 합산.
    n8n·Slack 연동 시 명세서 자동 작성에 활용."""
    from datetime import datetime, timezone

    _AMOUNT_RE = re.compile(r"(\d[\d,]*)만원")

    seen_sources: list[str] = []
    docs: list[dict] = []

    for alias in sorted(_df_namespace.keys()):
        source = _df_sources.get(alias, alias)
        if source in seen_sources:
            continue
        seen_sources.append(source)

        same_src = [a for a in _df_namespace if _df_sources.get(a) == source]
        total_count = sum(_count_valid_name_rows(_df_namespace[a]) for a in same_src)

        amount_str = _extract_total_from_source(alias)
        amount_int = 0
        if amount_str:
            m = _AMOUNT_RE.search(amount_str)
            if m:
                amount_int = int(m.group(1).replace(",", ""))

        # 목적: 파일명에서 번호·금액·괄호 제거
        core = re.sub(r"\s*[-–]\s*\d[\d,]*만원.*$", "", source)
        core = re.sub(r"\s*\.[a-zA-Z]+$", "", core)
        core = re.sub(r"\s*\([^)]*\)\s*", " ", core).strip()
        core = re.sub(r"^\d+\.\s*", "", core).strip()

        docs.append({
            "문서명": source,
            "목적": core,
            "인원": total_count,
            "총액": amount_str or "미확인",
            "총액_만원": amount_int,
        })

    total_people = sum(d["인원"] for d in docs)
    total_amount = sum(d["총액_만원"] for d in docs)

    return {
        "생성일시": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "전체합산": {
            "총인원": total_people,
            "총지원금액": f"{total_amount:,}만원",
        },
        "문서_목록": [
            {k: v for k, v in d.items() if k != "총액_만원"}
            for d in docs
        ],
    }


@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest, _: None = Depends(_verify_api_key)):
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="question이 비어있습니다.")
    try:
        route = _route(req.question)
        logger.info("[ROUTE] %s | question=%s", route, req.question[:50])
        if route == "PANDAS":
            answer, sources, actual_route = await _answer_pandas(req.question)
        else:
            answer, sources, actual_route = await _answer_vector(req.question)
        return ChatResponse(answer=answer, source=actual_route, sources=sources)
    except Exception as e:
        logger.exception("[CHAT] 처리 오류 | question=%s", req.question[:50])
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/chat/stream")
async def chat_stream(req: ChatRequest, _: None = Depends(_verify_api_key)):
    """스트리밍 응답 — n8n 없이 프론트에서 직접 붙일 때 사용."""
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="question이 비어있습니다.")

    async def generate() -> AsyncIterator[str]:
        try:
            route = _route(req.question)
            if route == "PANDAS":
                answer, _, _ = await _answer_pandas(req.question)
                yield answer
            else:
                async for chunk in get_rag_chain().astream(req.question):
                    yield chunk
        except Exception as e:
            logger.exception("Stream 처리 오류")
            yield f"\n[오류] {e}"

    return StreamingResponse(generate(), media_type="text/plain; charset=utf-8")


def _process_and_reload(file_path: str):
    """인제스트 후 DataFrame 저장소를 갱신한다."""
    process_file(file_path)
    _load_dataframes()


@app.post("/ingest", response_model=StatusResponse)
def ingest(req: IngestRequest, background_tasks: BackgroundTasks, _: None = Depends(_verify_api_key)):
    safe_path = _validate_ingest_path(req.file_path)
    if not os.path.exists(safe_path):
        raise HTTPException(status_code=404, detail=f"파일 없음: {safe_path}")
    background_tasks.add_task(_process_and_reload, safe_path)
    return StatusResponse(status="accepted", message=f"'{safe_path}' 처리를 시작했습니다.")


@app.post("/ingest/all", response_model=StatusResponse)
def ingest_all(background_tasks: BackgroundTasks, _: None = Depends(_verify_api_key)):
    if not os.path.exists(DATA_FOLDER):
        raise HTTPException(status_code=404, detail="data 폴더를 찾을 수 없습니다.")
    files = _find_files(DATA_FOLDER)
    if not files:
        return StatusResponse(status="ok", message="처리할 파일이 없습니다.")

    def _run():
        for fp in files:
            process_file(fp)
        _load_dataframes()

    background_tasks.add_task(_run)
    return StatusResponse(status="accepted", message=f"{len(files)}개 파일 처리를 시작했습니다.")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
