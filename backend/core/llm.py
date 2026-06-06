from __future__ import annotations

from typing import Optional

import chromadb
from langchain_ollama import OllamaLLM, OllamaEmbeddings
from langchain_chroma import Chroma
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough

from core.config import (
    OLLAMA_BASE_URL, OLLAMA_MODEL, EMBED_MODEL,
    CHROMA_HOST, CHROMA_PORT, COLLECTION_NAME,
)

_llm_rag:  Optional[OllamaLLM] = None
_llm_code: Optional[OllamaLLM] = None
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
        from rag.prompts import RAG_PROMPT
        _rag_chain = (
            {"context": get_retriever() | _fmt_docs, "question": RunnablePassthrough()}
            | RAG_PROMPT
            | get_llm_rag()
            | StrOutputParser()
        )
    return _rag_chain
