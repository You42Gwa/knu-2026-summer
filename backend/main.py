import glob
import os
import re
import sys
import time
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional
from urllib.request import urlopen
from urllib.error import URLError

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
from sqlalchemy import text

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database import engine
from utils.ingest import process_file, ensure_manifest_table

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
OLLAMA_BASE_URL      = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL         = os.getenv("OLLAMA_MODEL", "gemma4:e4b")
EMBED_MODEL          = os.getenv("EMBED_MODEL", "qwen3-embedding:0.6b")
CHROMA_HOST          = os.getenv("CHROMA_HOST", "localhost")
CHROMA_PORT          = int(os.getenv("CHROMA_PORT", "8000"))
COLLECTION_NAME      = "scholarship_rules"
DATA_FOLDER          = os.path.join(os.path.dirname(__file__), "data")
API_KEY              = os.getenv("API_KEY", "")
INGEST_ALLOWED_BASE  = os.path.realpath(os.getenv("INGEST_ALLOWED_BASE", DATA_FOLDER))

SCHEMA_CACHE_TTL = 300  # 스키마 캐시 유효 시간 (초)

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
_llm_rag: Optional[OllamaLLM] = None   # 답변 생성용 (temperature 낮춤)
_llm_sql: Optional[OllamaLLM] = None   # SQL 생성용 (결정론적)
_retriever = None
_rag_chain = None
_schema_cache: tuple[str, float] | None = None  # (schema_str, timestamp)


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


def get_llm_sql() -> OllamaLLM:
    global _llm_sql
    if _llm_sql is None:
        _llm_sql = OllamaLLM(
            base_url=OLLAMA_BASE_URL,
            model=OLLAMA_MODEL,
            temperature=0.0,   # SQL은 항상 동일한 결과가 나와야 함
            num_ctx=4096,
        )
    return _llm_sql


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
            search_kwargs={"k": 5, "fetch_k": 20, "lambda_mult": 0.7},
        )
    return _retriever


def get_rag_chain():
    global _rag_chain
    if _rag_chain is None:
        def fmt(docs):
            # 출처 정보 포함해 컨텍스트 구성
            parts = []
            for d in docs:
                src = d.metadata.get("source", "")
                page = d.metadata.get("page", "")
                label = f"[{src} p.{page}]" if page else f"[{src}]"
                parts.append(f"{label}\n{d.page_content}")
            return "\n\n".join(parts)

        _rag_chain = (
            {"context": get_retriever() | fmt, "question": RunnablePassthrough()}
            | RAG_PROMPT
            | get_llm_rag()
            | StrOutputParser()
        )
    return _rag_chain

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

_ROUTE_TEMPLATE = """\
다음 질문이 어떤 유형인지 한 단어로만 답하세요.

- 명단, 수치, 금액, 통계, 집계, 인원 등 표 데이터 조회 → SQL
- 규정, 문서, 안내, 절차, 내용 검색 → VECTOR

질문: {question}
유형(SQL 또는 VECTOR):"""

_RAG_TEMPLATE = """\
당신은 한국어 문서를 분석하는 전문 AI 어시스턴트입니다.
아래 참고 문서를 바탕으로 질문에 정확하고 상세하게 한국어로 답변하세요.
참고 문서에 없는 내용은 "해당 내용은 문서에서 확인할 수 없습니다."라고 답하세요.

참고 문서:
{context}

질문: {question}
답변:"""

_SQL_GEN_TEMPLATE = """\
당신은 PostgreSQL 전문가입니다. 아래 스키마를 보고 질문에 답하는 SELECT 쿼리를 작성하세요.
SQL 쿼리만 출력하세요. 설명이나 마크다운 없이 순수 SQL만 작성하세요.

규칙:
- 모든 테이블명과 컬럼명을 큰따옴표로 감싸세요: "테이블명"."컬럼명"
- ILIKE를 사용해 한국어 대소문자 무시 검색을 하세요
- 결과가 없을 수 있으므로 UNION으로 여러 테이블을 검색하세요

테이블 스키마:
{schema}

질문: {question}
SQL:"""

_SQL_ANSWER_TEMPLATE = """\
아래 데이터베이스 조회 결과를 바탕으로 질문에 한국어로 자연스럽게 답변하세요.
데이터가 없으면 "조회된 데이터가 없습니다."라고 답하세요.

질문: {question}
조회 결과:
{result}
답변:"""

RAG_PROMPT = PromptTemplate.from_template(_RAG_TEMPLATE)

# ---------------------------------------------------------------------------
# 라우팅 (키워드 기반 — LLM 호출 없이 즉시 판단)
# ---------------------------------------------------------------------------
_SQL_KEYWORDS = re.compile(
    r"명단|명|인원|몇\s*명|금액|얼마|통계|집계|합계|총\s*금액|지급액|수혜자|대상자|목록|리스트|누가|누구",
    re.IGNORECASE,
)

def _route(question: str) -> str:
    return "SQL" if _SQL_KEYWORDS.search(question) else "VECTOR"

# ---------------------------------------------------------------------------
# Vector RAG
# ---------------------------------------------------------------------------
_VECTOR_EMPTY_SIGNALS = ("해당 내용은 문서에서 확인할 수 없습니다", "문서에서 확인할 수 없")

async def _answer_vector(question: str, allow_sql_fallback: bool = True) -> str:
    logger.info("[VECTOR] 검색 시작 | question=%s", question[:50])
    answer = await get_rag_chain().ainvoke(question)
    logger.info("[VECTOR] 답변 생성 완료 | len=%d", len(answer))
    if allow_sql_fallback and any(s in answer for s in _VECTOR_EMPTY_SIGNALS):
        logger.info("[VECTOR→SQL] 유의미한 답변 없음, SQL 폴백 시도")
        sql_answer = await _answer_sql(question, allow_vector_fallback=False)
        if sql_answer and "없습니다" not in sql_answer and "오류" not in sql_answer:
            return sql_answer
    return answer

# ---------------------------------------------------------------------------
# SQL RAG
# ---------------------------------------------------------------------------
def _get_table_schema() -> str:
    global _schema_cache
    now = time.time()

    # TTL 캐시: 매 요청마다 DB 조회 방지
    if _schema_cache and now - _schema_cache[1] < SCHEMA_CACHE_TTL:
        return _schema_cache[0]

    parts = []
    with engine.connect() as conn:
        tables = conn.execute(text(
            "SELECT tablename FROM pg_tables "
            "WHERE schemaname = 'public' AND tablename != 'ingestion_manifest' "
            "ORDER BY tablename"
        )).fetchall()
        for (tbl,) in tables:
            cols = conn.execute(text(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = :t ORDER BY ordinal_position"
            ), {"t": tbl}).fetchall()
            col_str = ", ".join(f"{c}" for c, d in cols)
            # 샘플 1행 포함 → LLM이 컬럼 내용 파악 가능
            sample = conn.execute(text(
                f'SELECT * FROM "{tbl}" LIMIT 1'
            )).fetchone()
            sample_str = str(dict(sample._mapping)) if sample else ""
            parts.append(f"{tbl}({col_str})\n  예시: {sample_str}")

    schema = "\n".join(parts)
    _schema_cache = (schema, now)
    return schema


_SAFE_SQL_PATTERN = re.compile(
    r"\b(DROP|DELETE|UPDATE|INSERT|ALTER|TRUNCATE|CREATE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)

def _is_safe_sql(sql: str) -> bool:
    return sql.strip().upper().startswith("SELECT") and not _SAFE_SQL_PATTERN.search(sql)


async def _answer_sql(question: str, allow_vector_fallback: bool = True) -> str:
    schema = _get_table_schema()
    if not schema:
        return "현재 데이터베이스에 조회 가능한 테이블이 없습니다."

    logger.info("[SQL] 쿼리 생성 중 | question=%s", question[:50])
    raw_sql = await get_llm_sql().ainvoke(
        _SQL_GEN_TEMPLATE.format(schema=schema, question=question)
    )
    sql = re.sub(r"```(?:sql)?", "", raw_sql, flags=re.IGNORECASE).replace("```", "").strip()
    select_match = re.search(r"(SELECT\b.*)", sql, re.IGNORECASE | re.DOTALL)
    sql = select_match.group(1).strip() if select_match else sql
    logger.info("[SQL] 생성된 쿼리 | sql=%s", sql[:200])

    if not _is_safe_sql(sql):
        logger.warning("[SQL] 안전하지 않은 쿼리 차단 | sql=%s", sql[:100])
        return "안전하지 않은 쿼리가 생성되어 실행을 차단했습니다."

    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql)).fetchall()
        logger.info("[SQL] 조회 완료 | rows=%d", len(rows))
        raw_result = (
            "\n".join(str(dict(r._mapping)) for r in rows) if rows else ""
        )
    except Exception as e:
        logger.error("[SQL] 실행 오류 | err=%s | sql=%s", e, sql[:200])
        raw_result = ""

    if not raw_result and allow_vector_fallback:
        logger.info("[SQL→VECTOR] 결과 없음, VECTOR 폴백 시도")
        return await _answer_vector(question, allow_sql_fallback=False)

    if not raw_result:
        return "조회된 데이터가 없습니다."

    return (await get_llm_rag().ainvoke(
        _SQL_ANSWER_TEMPLATE.format(question=question, result=raw_result)
    )).strip()

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

    # LLM 워밍업 (첫 요청 지연 방지)
    try:
        logger.info("LLM 워밍업 중...")
        await get_llm_rag().ainvoke("안녕")
        logger.info("LLM 워밍업 완료")
    except Exception:
        logger.warning("LLM 워밍업 실패 (Ollama 미실행 가능)")

    yield

app = FastAPI(title="Local RAG Chatbot API", version="1.0.0", lifespan=lifespan)

# ---------------------------------------------------------------------------
# 스키마
# ---------------------------------------------------------------------------
class ChatRequest(BaseModel):
    question: str

class ChatResponse(BaseModel):
    answer: str
    source: str  # "vector" | "sql"

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
        "status": "ok",
        "llm_model":   OLLAMA_MODEL,
        "embed_model": EMBED_MODEL,
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


@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest, _: None = Depends(_verify_api_key)):
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="question이 비어있습니다.")
    try:
        route = _route(req.question)
        logger.info("[ROUTE] %s | question=%s", route, req.question[:50])
        answer = await _answer_sql(req.question) if route == "SQL" else await _answer_vector(req.question)
        return ChatResponse(answer=answer, source=route.lower())
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
            if route == "SQL":
                answer = await _answer_sql(req.question)
                yield answer
            else:
                async for chunk in get_rag_chain().astream(req.question):
                    yield chunk
        except Exception as e:
            logger.exception("Stream 처리 오류")
            yield f"\n[오류] {e}"

    return StreamingResponse(generate(), media_type="text/plain; charset=utf-8")


@app.post("/ingest", response_model=StatusResponse)
def ingest(req: IngestRequest, background_tasks: BackgroundTasks, _: None = Depends(_verify_api_key)):
    safe_path = _validate_ingest_path(req.file_path)
    if not os.path.exists(safe_path):
        raise HTTPException(status_code=404, detail=f"파일 없음: {safe_path}")
    background_tasks.add_task(process_file, safe_path)
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

    background_tasks.add_task(_run)
    return StatusResponse(status="accepted", message=f"{len(files)}개 파일 처리를 시작했습니다.")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
