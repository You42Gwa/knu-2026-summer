# 로컬 LLM 기반 하이브리드 RAG 문서 챗봇

## 1. 프로젝트 개요

- **과제명**: 로컬 LLM을 활용한 하이브리드 RAG 기반 사내 문서 처리 및 의사결정 지원 시스템
- **추진 배경**: 기존 텍스트 위주의 단순 RAG는 예산 계산 등 정형 데이터 기반의 수치 연산에서 할루시네이션을 유발함. 정형(표·수치)과 비정형(규정·문서) 데이터를 분리 저장하고 질의 유형에 따라 자동 라우팅하여 정확도를 높임.
- **최종 목표**: 오픈소스 로컬 LLM(Ollama)과 하이브리드 DB(Parquet + ChromaDB)를 결합하여 Slack 기반 자동화 챗봇 에이전트 구축.

---

## 2. 기술 스택

### Languages & Frameworks
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi)
![LangChain](https://img.shields.io/badge/LangChain-1C3C3C?style=for-the-badge&logo=langchain&logoColor=white)

### AI & Database
![Ollama](https://img.shields.io/badge/Ollama-000000?style=for-the-badge&logo=ollama&logoColor=white)
![Gemma](https://img.shields.io/badge/Gemma4-4285F4?style=for-the-badge&logo=google&logoColor=white)
![Qwen3](https://img.shields.io/badge/Qwen3--Embedding-FF6A00?style=for-the-badge&logo=huggingface&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![ChromaDB](https://img.shields.io/badge/ChromaDB-FF6D5A?style=for-the-badge&logo=chroma&logoColor=white)

### Automation & Infrastructure
![n8n](https://img.shields.io/badge/n8n-FF6D5A?style=for-the-badge&logo=n8n&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)

### Interface
![Slack](https://img.shields.io/badge/Slack-4A154B?style=for-the-badge&logo=slack&logoColor=white)

---

## 3. 시스템 아키텍처

```
Slack 메시지
  └─▶ n8n (트리거 / 전처리)
        └─▶ POST /chat  (FastAPI + X-API-Key 인증)
              └─▶ 키워드 기반 라우팅 판단
                    ├─ PANDAS ─▶ Parquet 로드 → ①이름 전수 검색 / ②키워드 직접 조회 / ③LLM 코드 생성(폴백) → 인메모리 실행 → 결과 포맷팅
                    └─ VECTOR ─▶ ChromaDB 검색 (bge-m3) → LLM 답변 생성
                                                             │
                                                       Ollama (gemma4:e4b)
  ◀─ n8n ◀─ FastAPI 응답 ◀──────────────────────────────────────────────┘
```

### 라우팅 기준

| 경로 | 트리거 키워드 예시 | 처리 방식 |
|---|---|---|
| **PANDAS** | 몇 명, 인원, 금액, 명단, 조회 | ①이름 전수 검색 → ②키워드 직접 조회 → ③LLM pandas 코드 생성(폴백) |
| **VECTOR** | 방법, 절차, 기준, 규정, 설명해 | ChromaDB 의미 검색 → LLM 답변 생성 |

> VECTOR 경로에서 유의미한 결과가 없으면 PANDAS 경로로 폴백합니다.

### 문서 적재 파이프라인

```
POST /ingest  또는  python utils/ingest.py
  ├─ PDF (텍스트) ─▶ 표 → Parquet + .meta.json  /  텍스트(표 제외) → ChromaDB
  ├─ PDF (스캔)   ─▶ pytesseract OCR (페이지별) → ChromaDB
  ├─ HWP         ─▶ hwp5html 변환 → 표 → Parquet + .meta.json  /  본문 → ChromaDB
  └─ XLSX        ─▶ 시트별 → Parquet + .meta.json

* PostgreSQL은 ingestion_manifest 테이블만 유지 (중복 적재 방지용 MD5 해시 추적)
* 적재 완료 후 _load_dataframes()로 인메모리 namespace 갱신
```

---

## 4. 폴더 구조

```
knu-2026-summer-rag/
├── backend/
│   ├── main.py              # FastAPI 서버 (라우팅, /chat, /ingest 엔드포인트)
│   ├── database.py          # PostgreSQL / ChromaDB 연결 설정
│   ├── check_chroma.py      # ChromaDB 상태 확인 유틸리티
│   ├── .env                 # [Git Ignored] 실제 환경변수 (backend/.env.example 참고)
│   ├── data/                # [Git Ignored] 입력 문서 (hwp, pdf, xlsx)
│   ├── dataframes/          # Parquet 캐시 + 메타데이터 (적재 시 자동 생성)
│   │   ├── df_tbl_*.parquet     # 문서에서 추출된 표 데이터
│   │   └── df_tbl_*.meta.json   # 원본 파일명·레이블 추적 메타데이터
│   ├── logs/
│   │   └── ingest.log       # 적재 처리 로그 (자동 생성, 5MB 로테이션)
│   ├── utils/
│   │   └── ingest.py        # 문서 파싱 및 DB 적재 파이프라인
│   └── tests/
│       ├── eval.py          # 평가 스크립트 (키워드 기반 정답률 측정)
│       ├── goldset.json     # 평가 질의셋 (easy / medium / hard 50개+)
│       ├── check_integrity.py   # 데이터 무결성 검증
│       └── make_goldset.py  # 평가 질의셋 생성기
├── .env                     # [Git Ignored] Docker Compose용 환경변수 (.env.example 참고)
├── .env.example             # 환경변수 템플릿 (복사 후 값 설정)
├── docker-compose.yml       # Ollama / PostgreSQL / ChromaDB / n8n 일괄 실행
├── my_workflow.json         # n8n Slack 워크플로우
├── requirements.txt         # Python 의존성
└── README.md
```

---

## 5. 시작하기

### 5-1. 환경변수 설정 (필수)

`.env.example`을 복사해 `.env`를 생성하고 값을 설정합니다.

```bash
# 루트 .env (Docker Compose용)
cp .env.example .env

# backend/.env (Python 앱용)
cp .env.example backend/.env
```

두 파일 모두 열어서 아래 항목을 반드시 변경하세요:

```dotenv
POSTGRES_PASSWORD=강력한_비밀번호로_변경
API_KEY=랜덤한_API_키로_변경
```

> `API_KEY`를 비워두면 인증 없이 동작합니다 (로컬 개발 환경에서만 권장).

---

### 5-2. 시스템 의존성 설치 (OCR 사용 시)

스캔 PDF OCR 처리를 위해 아래 도구를 **별도 설치**해야 합니다.

**Tesseract OCR** (한국어 언어팩 포함)
- Windows: https://github.com/UB-Mannheim/tesseract/wiki 에서 installer 다운로드
- 설치 시 "Additional language data" 목록에서 **Korean** 체크

**Poppler** (pdf2image 의존성, Windows만 필요)
- https://github.com/oschwartz10612/poppler-windows/releases 에서 다운로드
- 압축 해제 후 `bin/` 경로를 시스템 PATH에 추가

> OCR 미설치 시에도 텍스트 PDF / HWP / XLSX 처리는 정상 동작합니다.  
> 스캔 PDF 페이지는 경고 로그를 남기고 건너뜁니다.

---

### 5-3. 인프라 실행 (Docker)

루트 `.env` 파일이 있어야 `docker compose`가 실행됩니다.

```bash
docker compose up -d
```

| 서비스 | 포트 | 용도 |
|---|---|---|
| Ollama | 11434 | 로컬 LLM 서버 |
| PostgreSQL | 5432 | ingestion_manifest (중복 방지) |
| ChromaDB | 8000 | 벡터 DB |
| n8n | 5678 | 워크플로우 자동화 |

---

### 5-4. Ollama 모델 준비

> **주의**: `gemma4:e4b`는 최신 버전의 Ollama가 필요합니다.
> `docker pull ollama/ollama:latest && docker-compose up -d --force-recreate ollama` 로 업데이트 후 진행하세요.

```bash
# LLM (생성 모델)
docker exec ollama_server ollama pull gemma4:e4b

# 임베딩 모델
docker exec ollama_server ollama pull qwen3-embedding:0.6b
```

---

### 5-5. 백엔드 실행

```bash
cd backend
python -m venv venv

# Windows
venv\Scripts\activate
# Mac/Linux
source venv/bin/activate

pip install -r ../requirements.txt

uvicorn main:app --host 0.0.0.0 --port 8080 --reload
```

---

### 5-6. 문서 적재

**방법 A — API 호출** (`API_KEY` 설정 시 헤더 필수)

```bash
# 특정 파일 (backend/data/ 내부 경로만 허용)
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your_api_key" \
  -d '{"file_path": "/absolute/path/to/backend/data/file.pdf"}'

# data/ 폴더 전체
curl -X POST http://localhost:8080/ingest/all \
  -H "X-API-Key: your_api_key"
```

**방법 B — 직접 실행** (경로 제한 없음, 병렬 처리)

```bash
# backend/data/ 폴더에 문서를 넣고 실행
python utils/ingest.py
```

> 적재가 완료되면 `backend/dataframes/`에 Parquet 파일과 `.meta.json`이 생성됩니다.  
> 동일 파일을 다시 적재하면 MD5 해시로 중복을 감지하여 건너뜁니다.

---

## 6. API 엔드포인트

`API_KEY` 환경변수가 설정된 경우 `*` 표시 엔드포인트에 `X-API-Key` 헤더가 필요합니다.

| Method | Path | 인증 | 설명 |
|---|---|---|---|
| GET | `/health` | 불필요 | 서버·Ollama·ChromaDB 상태 확인 |
| POST | `/chat` | * | 질문 전송 → 자동 라우팅 → 답변 반환 |
| POST | `/chat/stream` | * | 스트리밍 답변 (프론트 직접 연동용) |
| POST | `/ingest` | * | 단일 파일 적재 (백그라운드, data/ 내부만 허용) |
| POST | `/ingest/all` | * | `data/` 폴더 전체 일괄 적재 (백그라운드) |

**`/chat` 요청/응답 예시**

```bash
curl -X POST http://localhost:8080/chat \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your_api_key" \
  -d '{"question": "2024년 장학금 예산 총액이 얼마야?"}'
```

```json
{
  "answer": "2024년 장학금 예산 총액은 ...",
  "source": "pandas"
}
```

> `source` 필드: `"pandas"` (정형 데이터 pandas 조회) | `"vector"` (문서 의미 검색)

**`/health` 응답 예시**

```json
{
  "status": "ok",
  "llm_model": "gemma4:e4b",
  "embed_model": "qwen3-embedding:0.6b",
  "dataframes": 5,
  "ollama": "ok",
  "chromadb": "ok"
}
```

---

## 7. 환경변수

`.env.example`을 복사해 `backend/.env` (Python 앱) 및 루트 `.env` (Docker Compose) 를 생성하세요.

| 변수 | 기본값 | 설명 |
|---|---|---|
| `POSTGRES_USER` | `admin` | PostgreSQL 사용자 |
| `POSTGRES_PASSWORD` | **(필수 설정)** | PostgreSQL 비밀번호 — 반드시 변경 |
| `POSTGRES_DB` | `rag_database` | PostgreSQL DB명 |
| `POSTGRES_HOST` | `localhost` | PostgreSQL 호스트 |
| `POSTGRES_PORT` | `5432` | PostgreSQL 포트 |
| `CHROMA_HOST` | `localhost` | ChromaDB 호스트 |
| `CHROMA_PORT` | `8000` | ChromaDB 포트 |
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama 서버 주소 |
| `OLLAMA_MODEL` | `gemma4:e4b` | 생성 LLM 모델 |
| `EMBED_MODEL` | `qwen3-embedding:0.6b` | 임베딩 모델 |
| `API_KEY` | *(비어있으면 인증 없음)* | `/chat`, `/ingest` 엔드포인트 보호용 API Key |
| `INGEST_ALLOWED_BASE` | `backend/data/` 절대경로 | `/ingest` API가 접근 가능한 최상위 디렉토리 |

---

## 8. 평가 (Evaluation)

`backend/tests/`에 평가 도구가 포함되어 있습니다.

```bash
cd backend
# 전체 평가 실행
python tests/eval.py

# 난이도별 필터링
python tests/eval.py --difficulty easy
python tests/eval.py --difficulty hard

# 카테고리별 필터링 (sql_명단, sql_금액, sql_인원, vector_규정 등)
python tests/eval.py --category sql_명단
```

`goldset.json`은 easy / medium / hard 3단계로 구성된 50개+ 질의셋으로,  
정형(pandas) / 비정형(vector) / 경계(negative) 케이스를 모두 포함합니다.

---

## 9. 협업 규칙

### Git 브랜치 전략
```
main ← develop ← feature/기능명
```

### 커밋 메시지 규칙
- `Feat`: 새로운 기능 추가
- `Fix`: 버그 수정
- `Docs`: 문서 수정
- `Refactor`: 기능 변경 없는 코드 구조 개선

### n8n 워크플로우
수정 후 반드시 JSON으로 내보내어 `my_workflow.json`으로 커밋.

---

## 10. 정량적 성과 목표

| 지표 | 목표 |
|---|---|
| 정형 데이터 질의 정답률 (Naive RAG 대비) | 90% 이상 |
| 할루시네이션 감소율 | 측정 및 기록 |
| End-to-End 응답 레이턴시 | 측정 및 기록 |

---

## 11. 팀원

| 역할 | 담당 |
|---|---|
| 팀장 | FastAPI 백엔드, 하이브리드 RAG 라우팅 설계, 인프라 통합 |
| 팀원 A | 데이터 엔지니어링 (문서 전처리 및 DB 적재 파이프라인) |
| 팀원 B | 자동화 파이프라인 (n8n · Slack 연동 워크플로우) |
| 팀원 C | AI 성능 평가 및 논문 작성 (프롬프트 튜닝, 평가 질의셋, KIPS) |
