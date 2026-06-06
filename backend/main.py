from __future__ import annotations

import glob
import logging
import os
import re
import shutil
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncIterator
from urllib.request import urlopen
from urllib.error import URLError

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))

import chromadb
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, UploadFile, File
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from langchain_ollama import OllamaEmbeddings

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 도메인 모듈 (config → security/llm/datastore/rag 순으로 의존)
from utils.ingest import process_file, ensure_manifest_table
from utils.manifest import get_manifest_status, get_all_manifest_entries, delete_manifest
from core.config import (
    OLLAMA_BASE_URL, OLLAMA_MODEL, EMBED_MODEL,
    CHROMA_HOST, CHROMA_PORT, DATA_FOLDER,
)
from core.security import _verify_api_key, _validate_ingest_path
from core.llm import get_llm_rag, get_rag_chain
from datastore.state import _df_namespace, _df_sources, _load_dataframes
from datastore.query import (
    _count_valid_name_rows,
    _extract_total_from_source,
    _extract_recipient_from_dfs,
    _extract_month_from_source,
)
from rag.router import _route
from rag.vector import _answer_vector
from rag.pandas_rag import _answer_pandas

logger = logging.getLogger("uvicorn.error")


# ---------------------------------------------------------------------------
# 파일 탐색 (재귀)
# ---------------------------------------------------------------------------
def _find_files(folder: str) -> list[str]:
    paths = []
    for ext in ("xlsx", "pdf", "hwp", "hwpx"):
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
    filename: str | None = None

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

        recipient = _extract_recipient_from_dfs(same_src)
        month_str = _extract_month_from_source(source)

        docs.append({
            "문서명": source,
            "목적": core,
            "인원": total_count,
            "총액": amount_str or "미확인",
            "총액_만원": amount_int,
            "지급처": recipient,
            "지출월": month_str,
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
        "전체합산_지급처": list(dict.fromkeys(
            d["지급처"] for d in docs if d["지급처"]
        )),
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


_ALLOWED_INGEST_EXTS = {"xlsx", "pdf", "hwp", "hwpx"}


@app.post("/ingest/upload", response_model=StatusResponse)
def ingest_upload(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    filename_override: str | None = None,
    _: None = Depends(_verify_api_key),
):
    """파일 바이너리를 직접 업로드받아 data 폴더에 저장 후 색인한다 (Slack 첨부 등).
    filename_override 쿼리 파라미터로 저장 파일명을 지정할 수 있다."""
    filename = os.path.basename(filename_override or file.filename or "")
    if not filename:
        raise HTTPException(status_code=400, detail="파일명이 없습니다.")
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext not in _ALLOWED_INGEST_EXTS:
        raise HTTPException(
            status_code=400,
            detail=f"지원하지 않는 형식: .{ext} (허용: {', '.join(sorted(_ALLOWED_INGEST_EXTS))})",
        )
    os.makedirs(DATA_FOLDER, exist_ok=True)
    dest = _validate_ingest_path(os.path.join(DATA_FOLDER, filename))
    try:
        with open(dest, "wb") as f:
            shutil.copyfileobj(file.file, f)
    finally:
        file.file.close()
    background_tasks.add_task(_process_and_reload, dest)
    return StatusResponse(status="accepted", message=f"'{filename}' 업로드 완료, 색인을 시작했습니다.", filename=filename)


@app.get("/documents")
def documents(_: None = Depends(_verify_api_key)):
    """색인된 문서 전체 목록과 상태를 반환한다. n8n·Slack 문서목록 명령용."""
    entries = get_all_manifest_entries()
    return {"count": len(entries), "files": entries}


@app.get("/status")
def ingest_status(source: str, _: None = Depends(_verify_api_key)):
    """파일명(source)으로 색인 상태를 조회한다. 업로드 후 n8n 폴링용.
    반환 status: IN_PROGRESS | SUCCESS | FAILED, 기록 없으면 404."""
    st = get_manifest_status(os.path.basename(source))
    if st is None:
        raise HTTPException(status_code=404, detail=f"'{source}' 색인 기록이 없습니다.")
    return st


@app.delete("/documents/{source}")
def delete_document(source: str, _: None = Depends(_verify_api_key)):
    """색인된 문서를 완전히 삭제한다 — ChromaDB·Parquet·manifest·data 파일 모두 제거."""
    from utils.chroma_store import delete_from_chroma
    from utils.parquet_store import drop_dataframe_by_source

    source = os.path.basename(source)

    chroma_deleted = delete_from_chroma(source)
    drop_dataframe_by_source(source)
    manifest_deleted = delete_manifest(source)

    if not manifest_deleted and not os.path.exists(os.path.join(DATA_FOLDER, source)):
        raise HTTPException(status_code=404, detail=f"'{source}' 문서를 찾을 수 없습니다.")

    # Parquet·manifest 삭제 후 즉시 메모리 갱신 (파일 락 여부와 무관)
    _load_dataframes()

    # data 파일 삭제 — Windows 파일 락 시 조용히 건너뜀
    data_path = os.path.join(DATA_FOLDER, source)
    file_existed = os.path.exists(data_path)
    if file_existed:
        try:
            os.remove(data_path)
        except PermissionError:
            logger.warning("data 파일 삭제 실패 (파일 락) — 다음 재시작 시 제거 필요: %s", data_path)

    return {
        "source": source,
        "chroma_deleted": chroma_deleted,
        "file_deleted": file_existed,
        "manifest_deleted": manifest_deleted,
    }


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
