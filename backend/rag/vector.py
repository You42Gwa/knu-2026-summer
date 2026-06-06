from __future__ import annotations

import logging
import os
import re

from langchain_core.output_parsers import StrOutputParser

from core.llm import get_llm_rag, get_llm_code, get_retriever, _fmt_docs
from rag.prompts import RAG_PROMPT, DOC_EXPLAIN_RAG_PROMPT, MULTI_QUERY_PROMPT
from rag.router import _VECTOR_OVERRIDE

logger = logging.getLogger("uvicorn.error")

# 문서 설명 질문 탐지 (목적·내용·요약·기준·기관 요청)
_DOC_EXPLAIN_RE = re.compile(
    r"문서의?\s*(목적|내용|설명)|설명해|어떤\s*(문서|내용)|요약해"
    r"|(?:지급|선발|지원)\s*(목적|기준|이유|방식|기관)"
    r"|(?:목적|내용|용도|기준|이유)\s*(?:이|가)?\s*(?:뭐야|뭐|무엇|어떤가|어떻게)",
    re.IGNORECASE,
)

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
        prompt = DOC_EXPLAIN_RAG_PROMPT
    else:
        prompt = RAG_PROMPT
    answer = await (prompt | get_llm_rag() | StrOutputParser()).ainvoke(
        {"context": context, "question": question}
    )
    logger.info("[VECTOR] 답변 생성 완료 | len=%d docs=%d", len(answer), len(docs))
    # 문서설명·목적 질문은 pandas 폴백 금지 (명단 테이블이 반환되면 더 나쁨)
    if allow_pandas_fallback and not _VECTOR_OVERRIDE.search(question) and any(s in answer for s in _VECTOR_EMPTY_SIGNALS):
        logger.info("[VECTOR→PANDAS] 유의미한 답변 없음, pandas 폴백 시도")
        # 지연 임포트로 순환 참조 방지
        from rag.pandas_rag import _answer_pandas
        pd_answer, pd_sources, _ = await _answer_pandas(question, allow_vector_fallback=False)
        if pd_answer and "없습니다" not in pd_answer and "오류" not in pd_answer:
            return pd_answer, pd_sources, "pandas"
    return answer, source_files, "vector"
