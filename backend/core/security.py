from __future__ import annotations

import os

from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader

from core.config import API_KEY, INGEST_ALLOWED_BASE

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def _verify_api_key(key: str = Security(_api_key_header)):
    if API_KEY and key != API_KEY:
        raise HTTPException(status_code=401, detail="유효하지 않은 API Key입니다.")


def _validate_ingest_path(file_path: str) -> str:
    abs_path = os.path.realpath(file_path)
    if not (abs_path == INGEST_ALLOWED_BASE or abs_path.startswith(INGEST_ALLOWED_BASE + os.sep)):
        raise HTTPException(
            status_code=400,
            detail=f"허용된 디렉토리 외부 파일에는 접근할 수 없습니다. (허용: {INGEST_ALLOWED_BASE})",
        )
    return abs_path
