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
![Qwen2.5](https://img.shields.io/badge/Qwen2.5--3B-FF6A00?style=for-the-badge&logo=huggingface&logoColor=white)
![BGE-M3](https://img.shields.io/badge/BGE--M3-4285F4?style=for-the-badge&logo=huggingface&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![ChromaDB](https://img.shields.io/badge/ChromaDB-FF6D5A?style=for-the-badge&logo=chroma&logoColor=white)

### Automation & Infrastructure
![n8n](https://img.shields.io/badge/n8n-FF6D5A?style=for-the-badge&logo=n8n&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)

### Interface
![Slack](https://img.shields.io/badge/Slack-4A154B?style=for-the-badge&logo=slack&logoColor=white)

---

## 3. Slack 명령어

봇을 멘션(`@봇`)하여 다음 명령을 사용합니다.

| 명령 | 설명 |
|---|---|
| `@봇 질문` | 자연어 질의응답 |
| `@봇 명세서 만들어줘` | 기부금 활용실적명세서 자동 작성 (Google Sheets) |
| `@봇 [파일 첨부]` | 파일 색인 (xlsx / pdf / hwp) |
| `@봇 문서목록` | 현재 색인된 문서 목록 및 상태 조회 |
| `@봇 삭제 파일명.xlsx` | 색인된 문서 완전 삭제 |
| `@봇 도움말` | 전체 기능 안내 |

---

### 명령어 사용 예시

> **※ 아래 이름·파일명·금액은 모두 데모 샘플 데이터 기준입니다.**  
> 실제 운영 시에는 적재된 문서의 내용에 따라 응답이 달라집니다.

#### 질의응답

```
@봇 성적우수 장학금 상반기 명단 알려줘
→ 1. 홍예준 (건축과 1학년, 250,000원)
   2. 장서연 (자동화과 1학년, 250,000원)
   ...총 16명

@봇 신입생 동문장학금 총 지급 금액은 얼마야
→ 신입생 동문장학금 총 지급 금액은 480만원입니다.

@봇 오태양 학생이 성적우수 장학금 받았어
→ 조회된 데이터가 없습니다.

@봇 신입생 동문장학금 선발 기준이 어떻게 돼
→ 당해 신입학생 전원에게 균등 지급하는 방식입니다.
   지급 기관은 한빛공업고등학교 동문회이며, ...
```

#### 명세서 자동 작성

```
@봇 명세서 만들어줘
→ 명세서를 작성 중입니다... (수초 소요)
→ ✅ 명세서 작성 완료: https://docs.google.com/spreadsheets/d/...
```

#### 파일 색인

```
@봇 [신입생 동문장학금 3월-480만원.xlsx 첨부]
→ 📥 색인을 시작합니다. 잠시 후 알려드릴게요.
→ (약 30초 후)
→ ✅ 신입생 동문장학금 3월-480만원.xlsx 색인 완료 (24건)
```

지원 형식: `xlsx`, `pdf`, `hwp`, `hwpx`

#### 문서 목록 조회

```
@봇 문서목록
→ 📂 현재 색인된 문서 목록 (6건)
   ✅ 신입생 동문장학금 3월-480만원.xlsx (24건)
   ✅ 성적우수 장학금 상반기 6월-320만원.pdf (16건)
   ✅ 성적우수 장학금 하반기 12월-280만원.pdf (14건)
   ✅ 체육특기생 지원금 9월-150만원.xlsx (10건)
   ✅ 학년말 성적우수 장학금 12월-200만원.xlsx (10건)
   ✅ 장학재단 특별장학금 9월-240만원.hwp (12건)
```

#### 문서 삭제

```
@봇 삭제 신입생 동문장학금 3월-480만원.xlsx
→ ✅ 삭제 완료: 신입생 동문장학금 3월-480만원.xlsx
```

> 파일명은 `@봇 문서목록`으로 먼저 확인하세요. 파일명이 정확히 일치해야 삭제됩니다.

#### 도움말

```
@봇 도움말
→ 사용 가능한 명령어 안내 (위 표 내용 전송)
```

---

## 4. 핵심 기능

### 기능 1 — 질의응답 (`/chat`)

자연어 질문을 자동 분류하여 두 가지 경로로 처리합니다.

| 경로 | 트리거 키워드 | 처리 방식 |
|---|---|---|
| **PANDAS** | 명단, 몇 명, 금액, 인원, 종목 등 | Parquet 직접 조회 → 집계/필터링 → 결과 반환 |
| **VECTOR** | 방법, 절차, 설명, 목적, 내용, 기준 등 | ChromaDB 의미 검색 → LLM 답변 생성 |

```
질문 예시:
  "하반기 장학금 1학년 대상자 명단을 알려줘"   → PANDAS
  "신입생 장학금 선발 기준이 뭐야?"            → VECTOR
```

**이름 기반 학생 검색 (LLM 코드 생성 생략)**

질문에서 한국어 이름을 자동 감지하면 LLM 코드 생성 없이 전체 문서를 전수 검색합니다. 이름이 없으면 "조회된 데이터가 없습니다"를 즉시 반환합니다.

```
"오태양 학생이 성적우수 장학금 받았어?" → 이름 추출 → 전체 DF 검색 → 결과 없으면 "없음" 반환
"장서연 학생이 상반기에 있어?"          → 이름 추출 → 상반기 DF에서 발견 → 해당 행 반환
```

**Fallback 체인**

한쪽 경로에서 유효한 결과가 없으면 자동으로 반대 경로를 시도합니다.

```
PANDAS → 결과 없음 → VECTOR 자동 시도 (규정·설명 추출)
VECTOR → 문서에서 확인 불가 → PANDAS 자동 시도 (정형 데이터 조회)
```

---

### 기능 2 — 명세서 자동 작성 (`/summary` + n8n)

적재된 모든 문서에서 **목적·인원·지원금액·지급처·지출월**을 자동 추출하여 Google Sheets 기부금 활용실적명세서 템플릿에 자동 입력합니다.

```
@봇 명세서 만들어줘
  → /summary 호출 → 문서별 인원·금액·지급처 집계
  → Google Drive 템플릿 복사 → Sheets 자동 입력
  → 완성된 시트 링크를 Slack으로 회신
```

---

### 기능 3 — 파일 색인 (`/ingest/upload`)

Slack에서 파일을 첨부하여 봇을 멘션하면 자동으로 문서를 색인합니다.

```
@봇 [xlsx / pdf / hwp 파일 첨부]
  → n8n 파일 감지 → Slack 다운로드 → POST /ingest/upload
  → "색인 시작" 안내 → 30초 후 GET /status 폴링
  → "✅ N건 색인 완료" 또는 "⚠️ 실패" 결과 회신
```

지원 형식: `xlsx`, `pdf`, `hwp`, `hwpx`

---

### 기능 4 — 문서 목록 조회 (`/documents`)

```
@봇 문서목록
  → GET /documents → 색인 상태별 이모지 포함 목록 Slack 회신
  ✅ 신입생 동문장학금 3월-480만원.xlsx (4건)
  ✅ 장학재단 특별장학금 9월-240만원.hwp (3건)
  ...
```

---

### 기능 5 — 문서 삭제 (`/documents/{source}`)

색인된 문서를 ChromaDB·Parquet·manifest·data 파일까지 완전 삭제하고 인메모리 DataFrame을 즉시 갱신합니다.

```
@봇 삭제 취업특기생 지원금 11월-240만원.xlsx
  → DELETE /documents/{source}
  → ChromaDB 벡터 삭제 → Parquet 삭제 → manifest 삭제 → 메모리 재로드
  → "✅ 삭제 완료" 또는 "❌ 실패" 결과 회신
```

---

## 5. 시스템 아키텍처

```
Slack 멘션
  └─▶ n8n (Slack Trigger → Edit Fields → 분기 체인)
        │
        ├─▶ [파일 첨부]       POST /ingest/upload
        │     └─▶ data/ 저장 → 백그라운드 색인 (PDF/HWP/XLSX 파서)
        │           └─▶ GET /status 폴링 (30s) → Slack 완료 알림
        │
        ├─▶ [문서목록]        GET /documents
        │     └─▶ manifest 전체 조회 → 상태 이모지 포함 목록 회신
        │
        ├─▶ [삭제]           DELETE /documents/{source}
        │     └─▶ ChromaDB·Parquet·manifest·data 일괄 삭제 → 메모리 갱신
        │
        ├─▶ [도움말]          n8n 정적 텍스트 회신
        │
        ├─▶ [명세서]          GET /summary
        │     └─▶ 문서별 인원·금액·목적·지급처·지출월 집계
        │           → Google Drive 템플릿 복사 → Sheets 자동 입력 → 링크 회신
        │
        └─▶ [질의]           POST /chat
              └─▶ 키워드 기반 라우팅
                    ├─ PANDAS ─▶ Parquet 로드 → 이름검색 / 키워드 조회 / LLM 코드 생성 → 포맷팅
                    └─ VECTOR ─▶ ChromaDB 검색 (bge-m3) → LLM 답변 생성 (qwen2.5:3b)
```

### 문서 적재 파이프라인

```
POST /ingest/upload  또는  POST /ingest  또는  POST /ingest/all
  ├─ XLSX ─▶ 시트별 표 → Parquet + .meta.json
  ├─ PDF  ─▶ 표 → Parquet  /  텍스트(표 제외) → ChromaDB
  │          스캔 PDF → pytesseract OCR → ChromaDB
  └─ HWP  ─▶ pyhwpx COM 자동화 → 표 → Parquet + ChromaDB (표·개요 청크)

* 각 문서마다 [문서 개요] 청크를 ChromaDB에 추가 주입 (vector 검색 품질 향상)
* PostgreSQL: ingestion_manifest 테이블 (중복 적재 방지용 MD5 해시)
* 적재 완료 후 _load_dataframes()로 인메모리 namespace 즉시 갱신
```

---

## 6. 폴더 구조

```
knu-2026-summer-rag/
├── backend/
│   ├── main.py              # FastAPI 진입점 (엔드포인트 + 스키마)
│   ├── database.py          # PostgreSQL / ChromaDB 연결 설정
│   ├── .env                 # [Git Ignored] 환경변수
│   ├── data/                # [Git Ignored] 입력 문서 (hwp, pdf, xlsx)
│   ├── dataframes/          # [Git Ignored] Parquet 캐시 + 메타데이터
│   ├── core/
│   │   ├── config.py        # 환경변수 로드
│   │   ├── llm.py           # Ollama LLM · retriever 싱글턴
│   │   └── security.py      # API Key 인증 · 경로 검증
│   ├── datastore/
│   │   ├── state.py         # 공유 DataFrame namespace 로드/보관
│   │   ├── schema.py        # LLM용 스키마 문자열 생성
│   │   └── query.py         # 이름 검색 · 키워드 필터 · 직접 조회
│   ├── pandas_engine/
│   │   ├── executor.py      # 샌드박스 exec (금지 패턴 차단)
│   │   └── formatter.py     # 결과 포맷팅
│   ├── rag/
│   │   ├── router.py        # 질의 라우팅 (PANDAS / VECTOR)
│   │   ├── prompts.py       # 프롬프트 템플릿
│   │   ├── vector.py        # 벡터 RAG (의미 검색 → LLM)
│   │   └── pandas_rag.py    # pandas RAG (검색 → 집계 → 포맷)
│   ├── utils/
│   │   ├── ingest.py        # 적재 진입점 (유형별 파서로 위임)
│   │   ├── manifest.py      # PostgreSQL manifest CRUD · 상태 조회
│   │   ├── parquet_store.py # Parquet 저장 / source 기반 삭제
│   │   ├── chroma_store.py  # ChromaDB 저장 / 삭제
│   │   ├── text_utils.py    # 텍스트 청킹 · 문서 개요 생성
│   │   ├── table_parser.py  # 표 파싱 · 정제 · 이름 정규화
│   │   ├── hwp_extract.py   # HWP 표 추출 헬퍼 (pyhwpx subprocess 격리)
│   │   └── parsers/
│   │       ├── pdf_parser.py
│   │       ├── xlsx_parser.py
│   │       └── hwp_parser.py
│   └── tests/
│       ├── eval.py                # 평가 스크립트 (키워드 기반 정답률 측정)
│       ├── goldset.json           # 골드셋 (90케이스 — sql_명단/인원/금액/vector/edge_case)
│       ├── generate_demo_data.py  # 데모 데이터 생성 (Excel/PDF)
│       └── results/               # [Git Ignored] eval 결과 리포트 (마크다운)
├── .env.example             # 환경변수 템플릿
├── docker-compose.yml       # Ollama / PostgreSQL / ChromaDB / n8n 일괄 실행
├── my_workflow.json         # n8n Slack 워크플로우
├── requirements.txt
└── README.md
```

---

## 7. 시작하기

### 7-1. 환경변수 설정

`.env.example`을 복사해 `.env`를 생성하고 값을 설정합니다.

```bash
cp .env.example .env
cp .env.example backend/.env
```

반드시 변경해야 할 항목:

```dotenv
POSTGRES_PASSWORD=강력한_비밀번호로_변경
API_KEY=랜덤한_API_키로_변경
```

---

### 7-2. HWP 파일 처리 (Windows 전용)

HWP 파일 적재는 **한글과컴퓨터 한글**이 설치된 Windows에서만 동작합니다.  
`pyhwpx`가 COM 자동화로 자동 처리하며, 한글 미설치 환경에서는 HWP 파일 적재를 건너뜁니다.

---

### 7-3. 시스템 의존성 (OCR 사용 시)

**Tesseract OCR** (한국어 언어팩 포함)
- Windows: https://github.com/UB-Mannheim/tesseract/wiki 에서 installer 다운로드
- 설치 시 "Additional language data" → **Korean** 체크

**Poppler** (pdf2image 의존성, Windows만 필요)
- https://github.com/oschwartz10612/poppler-windows/releases 에서 다운로드
- 압축 해제 후 `bin/` 경로를 시스템 PATH에 추가

---

### 7-4. 인프라 실행 (Docker)

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

### 7-5. Ollama 모델 준비

```bash
docker exec ollama_server ollama pull qwen2.5:3b
docker exec ollama_server ollama pull bge-m3
```

---

### 7-6. 백엔드 실행

```bash
cd backend
python -m venv venv
venv\Scripts\activate      # Windows
source venv/bin/activate   # Mac/Linux

pip install -r ../requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8080 --reload
```

---

### 7-7. 문서 적재

**방법 1 — Slack 파일 첨부 (권장)**
```
@봇 [파일 첨부]  → 자동 색인
```

**방법 2 — API (파일 업로드)**
```bash
curl -X POST "http://localhost:8080/ingest/upload?filename_override=파일명.xlsx" \
  -F "file=@파일명.xlsx"

# 색인 상태 확인
curl "http://localhost:8080/status?source=파일명.xlsx"
```

**방법 3 — API (서버 경로 지정)**
```bash
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -d '{"file_path": "data/파일명.xlsx"}'
```

---

### 7-8. n8n 워크플로우 설정

1. `http://localhost:5678` 접속
2. 상단 메뉴 → **Import from file** → `my_workflow.json` 선택
3. 다음 credential 연결:
   - **Slack account** (Bot Token): Download File, Send* 노드
   - **Google Drive account**: Copy Template1 노드
   - **Google Sheets account**: Append* 노드
4. Slack 앱 설정에서 Bot Token Scopes 확인:
   - `app_mentions:read`, `chat:write`, `files:read`

---

## 8. API 엔드포인트

`API_KEY` 환경변수 미설정 시 인증 없이 사용 가능합니다. 설정 시 `*` 표시 엔드포인트에 `X-API-Key` 헤더가 필요합니다.

| Method | Path | 인증 | 설명 |
|---|---|---|---|
| GET | `/health` | 불필요 | 서버·Ollama·ChromaDB 상태 확인 |
| POST | `/chat` | * | 질문 → 자동 라우팅 → 답변 반환 |
| POST | `/chat/stream` | * | 스트리밍 답변 (프론트 직접 연동용) |
| GET | `/summary` | * | 전체 문서 명세서 (인원·금액·목적·지급처·지출월 집계) |
| GET | `/documents` | * | 색인된 문서 전체 목록 + 상태 조회 |
| DELETE | `/documents/{source}` | * | 문서 완전 삭제 (ChromaDB·Parquet·manifest·data) |
| POST | `/ingest` | * | 단일 파일 적재 — 서버 경로 지정 (data/ 내부만 허용) |
| POST | `/ingest/upload` | * | 파일 업로드 적재 — multipart (`filename_override` 쿼리 파라미터로 파일명 지정 가능) |
| POST | `/ingest/all` | * | `data/` 폴더 전체 일괄 적재 |
| GET | `/status` | * | 파일별 색인 상태 조회 (`?source=파일명`) |

### 응답 예시

```bash
# 문서 목록
curl http://localhost:8080/documents
```
```json
{
  "count": 6,
  "files": [
    {"source": "신입생 동문장학금 3월-480만원.xlsx", "status": "SUCCESS", "chroma_doc_count": 4},
    {"source": "장학재단 특별장학금 9월-240만원.hwp", "status": "SUCCESS", "chroma_doc_count": 3}
  ]
}
```

```bash
# 문서 삭제
curl -X DELETE "http://localhost:8080/documents/파일명.xlsx"
```
```json
{"source": "파일명.xlsx", "chroma_deleted": 3, "file_deleted": true, "manifest_deleted": true}
```

```bash
# 질의응답
curl -X POST http://localhost:8080/chat \
  -H "Content-Type: application/json" \
  -d '{"question": "신입생 장학금 총액이 얼마야?"}'
```
```json
{"answer": "4,800,000원입니다.", "source": "pandas", "sources": ["신입생 동문장학금 3월-480만원.xlsx"]}
```

---

## 9. 환경변수

| 변수 | 기본값 | 설명 |
|---|---|---|
| `POSTGRES_USER` | `admin` | PostgreSQL 사용자 |
| `POSTGRES_PASSWORD` | **(필수 설정)** | PostgreSQL 비밀번호 |
| `POSTGRES_DB` | `rag_database` | PostgreSQL DB명 |
| `POSTGRES_HOST` | `localhost` | PostgreSQL 호스트 |
| `POSTGRES_PORT` | `5432` | PostgreSQL 포트 |
| `CHROMA_HOST` | `localhost` | ChromaDB 호스트 |
| `CHROMA_PORT` | `8000` | ChromaDB 포트 |
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama 서버 주소 |
| `OLLAMA_MODEL` | `qwen2.5:3b` | 생성 LLM 모델 |
| `EMBED_MODEL` | `bge-m3` | 임베딩 모델 |
| `API_KEY` | *(비어있으면 인증 없음)* | 엔드포인트 보호용 API Key |
| `INGEST_ALLOWED_BASE` | `backend/data/` 절대경로 | `/ingest` API 접근 가능 디렉토리 |
| `COLLECTION_NAME` | `scholarship_rules` | ChromaDB 컬렉션명 |

---

## 10. 평가 결과

모델: `qwen2.5:3b` (생성) + `bge-m3` (임베딩) · 평가 방식: 키워드 재현율 기반 pass/fail

### 실제 데이터 기반 성능 개선 이력 (v1.x)

초기 구축 단계에서 실제 업무 문서를 대상으로 측정한 결과입니다.

| 버전 | 정확도 | 주요 변경 |
|---|:---:|---|
| v1.0 초기 | 43% | SQL 기반 단순 조회만 지원 |
| v1.8 | **81%** | 하이브리드 RAG 도입 (pandas + vector), 명세서 기능 추가 |

> commit `6ce9e6b` — "하이브리드 RAG 성능 개선 v1.8 + 명세서 기능 추가 (43%→81%)"

---

### 데모 데이터 골드셋 — 기본 25케이스 (v2.0)

표준화된 데모 데이터와 골드셋으로 재현 가능한 형태로 측정한 결과입니다.

| 카테고리 | 통과 | 전체 | 정확도 |
|---|:---:|:---:|:---:|
| sql_명단 | 7 | 7 | 100% |
| sql_인원 | 4 | 4 | 100% |
| sql_금액 | 6 | 6 | 100% |
| vector_문서 | 7 | 8 | 88% |
| **전체** | **24** | **25** | **96%** |

> 라우팅 정확도 100% · 평균 응답 시간 3.6s

---

### 데모 데이터 골드셋 — 확장 84케이스 (v2.4 — 최신)

복합 집계·엣지 케이스 등 더 어려운 질의를 포함해 확장한 결과입니다.

| 카테고리 | 통과 | 전체 | 정확도 |
|---|:---:|:---:|:---:|
| sql_명단 | 19 | 19 | 100% |
| sql_인원 | 15 | 15 | 100% |
| sql_금액 | 21 | 22 | 95% |
| vector_문서 | 22 | 22 | 100% |
| edge_case | 6 | 6 | 100% |
| **전체** | **83** | **84** | **99%** |

> 라우팅 정확도 100% · 평균 응답 시간 3.2s  
> 알려진 한계: 크로스 도큐먼트 합산 미지원 / TC053 (등급 컬럼 부재로 1등급 필터 불가)

---

## 11. 협업 규칙

### Git 브랜치 전략
```
main ← develop ← feature/기능명
```

### 커밋 메시지 규칙
- `feat`: 새로운 기능 추가
- `fix`: 버그 수정
- `docs`: 문서 수정
- `refactor`: 기능 변경 없는 코드 구조 개선

### n8n 워크플로우
수정 후 반드시 JSON으로 내보내어 `my_workflow.json`으로 커밋.

---

## 12. 팀원

| 역할 | 담당 |
|---|---|
| 팀장 | FastAPI 백엔드, 하이브리드 RAG 라우팅 설계, 인프라 통합 |
| 팀원 A | 데이터 엔지니어링 (문서 전처리 및 DB 적재 파이프라인) |
| 팀원 B | 자동화 파이프라인 (n8n · Slack 연동 워크플로우) |
| 팀원 C | AI 성능 평가 및 논문 작성 (프롬프트 튜닝, 평가 질의셋, KCC 2026) |
