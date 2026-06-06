from __future__ import annotations

from pydantic import BaseModel


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
