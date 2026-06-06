from __future__ import annotations

import os

OLLAMA_BASE_URL     = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL        = os.getenv("OLLAMA_MODEL", "gemma4:e4b")
EMBED_MODEL         = os.getenv("EMBED_MODEL", "qwen3-embedding:0.6b")
CHROMA_HOST         = os.getenv("CHROMA_HOST", "localhost")
CHROMA_PORT         = int(os.getenv("CHROMA_PORT", "8000"))
COLLECTION_NAME     = os.getenv("COLLECTION_NAME", "scholarship_rules")
DATA_FOLDER         = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
API_KEY             = os.getenv("API_KEY", "")
INGEST_ALLOWED_BASE = os.path.realpath(os.getenv("INGEST_ALLOWED_BASE", DATA_FOLDER))

ORG_CORP_NAME       = os.getenv("ORG_CORP_NAME", "")
ORG_BUSINESS_NO     = os.getenv("ORG_BUSINESS_NO", "")
ORG_REPRESENTATIVE  = os.getenv("ORG_REPRESENTATIVE", "")
ORG_DONATION_TYPE   = os.getenv("ORG_DONATION_TYPE", "")
ORG_EMAIL           = os.getenv("ORG_EMAIL", "")
ORG_BUSINESS_YEAR   = os.getenv("ORG_BUSINESS_YEAR", "")
ORG_PHONE           = os.getenv("ORG_PHONE", "")
ORG_DESIGNATED_DATE = os.getenv("ORG_DESIGNATED_DATE", "")
ORG_ADDRESS         = os.getenv("ORG_ADDRESS", "")
