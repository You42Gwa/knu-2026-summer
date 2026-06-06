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

## 3. 핵심 기능

### 기능 1 — 질의 응답 (`/chat`)

사용자가 자연어로 질문하면 AI가 적재된 문서를 분석해 답변을 도출합니다.

```
질문 예시:
  "하반기 장학금 1학년 대상자 명단을 알려줘"
  "1학년 장학생이 몇 명이야?"
  "신입생 장학금 지급 금액은 얼마야?"
  "해당 문서의 목적이나 내용을 설명해줘"
```

질의 유형을 자동 분류하여 두 가지 경로로 처리합니다.

| 경로 | 트리거 | 처리 방식 |
|---|---|---|
| **PANDAS** | 명단, 몇 명, 금액, 인원, 종목 등 | Parquet 직접 조회 → 집계/필터링 → 결과 반환 |
| **VECTOR** | 방법, 절차, 설명, 목적, 내용 등 | ChromaDB 의미 검색 → LLM 답변 생성 |

---

### 기능 2 — 문서 명세서 생성 (`/summary`)

적재된 모든 문서에서 **목적·인원·지원 금액**을 자동 추출하여 구조화된 명세서를 반환합니다.

```json
{
  "생성일시": "2026-06-03 ...",
  "전체합산": {
    "총인원": 198,
    "총지원금액": "3,760만원"
  },
  "문서_목록": [
    {
      "문서명": "장학금 지급 대상자 명단.pdf",
      "목적": "장학금 지급 대상자",
      "인원": 29,
      "총액": "760만원"
    },
    ...
  ]
}
```

n8n 워크플로우와 연동하여 Slack 보고서 자동 발송 또는 양식 문서 자동 작성에 활용합니다.

---

## 4. 시스템 아키텍처

```
Slack 메시지
  └─▶ n8n (트리거 / 전처리)
        ├─▶ POST /chat  (질의 응답)
        │     └─▶ 키워드 기반 라우팅 판단
        │           ├─ PANDAS ─▶ Parquet 로드 → ①이름 전수 검색 / ②키워드 직접 조회 / ③LLM 코드 생성(폴백) → 결과 포맷팅
        │           └─ VECTOR ─▶ ChromaDB 검색 (bge-m3) → LLM 답변 생성
        │                                                    │
        │                                             Ollama (qwen2.5:3b)
        └─▶ GET  /summary  (문서 명세서)
              └─▶ 적재 문서별 인원·금액·목적 자동 집계 → JSON 반환

  ◀─ n8n ◀─ FastAPI 응답 ◀──────────────────────────────────────────────┘
```

### 문서 적재 파이프라인

```
POST /ingest  또는  python utils/ingest.py
  ├─ PDF (텍스트) ─▶ 표 → Parquet + .meta.json  /  텍스트(표 제외) → ChromaDB
  ├─ PDF (스캔)   ─▶ pytesseract OCR (페이지별) → ChromaDB
  ├─ HWP         ─▶ pyhwpx COM 자동화 → 표 → Parquet + .meta.json  /  본문 → ChromaDB
  └─ XLSX        ─▶ 시트별 → Parquet + .meta.json

* 각 문서마다 [문서 개요] 청크(목적·금액·항목 요약)를 ChromaDB에 추가 주입
* PostgreSQL은 ingestion_manifest 테이블만 유지 (중복 적재 방지용 MD5 해시 추적)
* 적재 완료 후 _load_dataframes()로 인메모리 namespace 갱신
```

---

## 5. 폴더 구조

```
knu-2026-summer-rag/
├── backend/
│   ├── main.py              # FastAPI 서버 (라우팅, /chat, /summary, /ingest 엔드포인트)
│   ├── database.py          # PostgreSQL / ChromaDB 연결 설정
│   ├── check_chroma.py      # ChromaDB 상태 확인 유틸리티
│   ├── .env                 # [Git Ignored] 실제 환경변수
│   ├── data/                # [Git Ignored] 입력 문서 (hwp, pdf, xlsx)
│   ├── dataframes/          # [Git Ignored] Parquet 캐시 + 메타데이터
│   ├── logs/                # [Git Ignored] 적재 처리 로그
│   ├── utils/
│   │   ├── ingest.py        # 문서 파싱 및 DB 적재 파이프라인
│   │   └── hwp_extract.py   # HWP 표 추출 헬퍼 (pyhwpx subprocess 격리)
│   └── tests/
│       ├── eval.py          # 평가 스크립트 (키워드 기반 정답률 측정)
│       ├── make_goldset.py  # 골드셋 자동 생성 스크립트
│       ├── generate_demo_data.py  # 데모 데이터 생성 (Excel/PDF)
│       ├── version.md       # 개선 이력 및 알려진 문제
│       ├── compare.py       # 두 eval 결과 비교 유틸리티
│       └── check_integrity.py   # 데이터 무결성 검증
├── .env.example             # 환경변수 템플릿
├── docker-compose.yml       # Ollama / PostgreSQL / ChromaDB / n8n 일괄 실행
├── my_workflow.json         # n8n Slack 워크플로우
├── requirements.txt
└── README.md
```

---

## 6. 시작하기

### 6-1. 환경변수 설정 (필수)

`.env.example`을 복사해 `.env`를 생성하고 값을 설정합니다.

```bash
cp .env.example .env
cp .env.example backend/.env
```

두 파일 모두 아래 항목을 반드시 변경하세요:

```dotenv
POSTGRES_PASSWORD=강력한_비밀번호로_변경
API_KEY=랜덤한_API_키로_변경
```

---

### 6-2. HWP 파일 처리 설정 (Windows 전용)

HWP 파일 적재는 **한글과컴퓨터 한글** 소프트웨어가 설치된 Windows에서만 동작합니다.  
한글이 설치되어 있으면 `pyhwpx`가 COM 자동화로 자동 처리합니다.  
한글 미설치 환경에서는 HWP 파일 적재가 건너뜁니다.

---

### 6-3. 시스템 의존성 설치 (OCR 사용 시)

**Tesseract OCR** (한국어 언어팩 포함)
- Windows: https://github.com/UB-Mannheim/tesseract/wiki 에서 installer 다운로드
- 설치 시 "Additional language data" 목록에서 **Korean** 체크

**Poppler** (pdf2image 의존성, Windows만 필요)
- https://github.com/oschwartz10612/poppler-windows/releases 에서 다운로드
- 압축 해제 후 `bin/` 경로를 시스템 PATH에 추가

---

### 6-3. 인프라 실행 (Docker)

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

### 6-4. Ollama 모델 준비

```bash
docker exec ollama_server ollama pull qwen2.5:3b
docker exec ollama_server ollama pull bge-m3
```

---

### 6-5. 백엔드 실행

```bash
cd backend
python -m venv venv
venv\Scripts\activate      # Windows
source venv/bin/activate   # Mac/Linux

pip install -r ../requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8080 --reload
```

---

### 6-6. 문서 적재

```bash
# data/ 폴더에 문서를 넣고 실행
python utils/ingest.py
```

---

## 7. API 엔드포인트

`API_KEY` 환경변수가 설정된 경우 `*` 표시 엔드포인트에 `X-API-Key` 헤더가 필요합니다.

| Method | Path | 인증 | 설명 |
|---|---|---|---|
| GET | `/health` | 불필요 | 서버·Ollama·ChromaDB 상태 확인 |
| GET | `/summary` | * | **모든 문서 명세서** (인원·금액·목적 자동 집계) |
| POST | `/chat` | * | 질문 전송 → 자동 라우팅 → 답변 반환 |
| POST | `/chat/stream` | * | 스트리밍 답변 (프론트 직접 연동용) |
| POST | `/ingest` | * | 단일 파일 적재 (백그라운드, data/ 내부만 허용) |
| POST | `/ingest/all` | * | `data/` 폴더 전체 일괄 적재 (백그라운드) |

### `/summary` 응답 예시

```bash
curl http://localhost:8080/summary -H "X-API-Key: your_api_key"
```

```json
{
  "생성일시": "2026-06-03 00:00 UTC",
  "전체합산": {
    "총인원": 198,
    "총지원금액": "3,760만원"
  },
  "문서_목록": [
    {
      "문서명": "1. 2024학년도 신입생 동문회 장학금 지급 대상자(본교 상반기)-760만원.pdf",
      "목적": "2024학년도 신입생 동문회 장학금 지급 대상자",
      "인원": 29,
      "총액": "760만원"
    }
  ]
}
```

### `/chat` 요청/응답 예시

```bash
curl -X POST http://localhost:8080/chat \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your_api_key" \
  -d '{"question": "2024년 장학금 예산 총액이 얼마야?"}'
```

```json
{
  "answer": "지급 금액은 760만원입니다.",
  "source": "pandas",
  "sources": ["장학금 지급 대상자 명단.pdf"]
}
```

---

## 8. 환경변수

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

---

## 9. 평가 결과

데모 데이터 기반 골드셋(25케이스)으로 측정한 성능입니다. 모델: `qwen2.5:3b` + `bge-m3`

| 카테고리 | 케이스 수 | 비고 |
|---|---|---|
| sql_명단 | 7 | pandas 라우팅 |
| sql_금액 | 6 | pandas 라우팅 |
| sql_인원 | 4 | pandas 라우팅 |
| vector_문서 | 8 | ChromaDB + LLM |

> 상세 결과는 `backend/tests/results/` 참조.  
> 알려진 문제: vector 성능 낮음 (소형 모델 한계), 크로스 도큐먼트 합산 미지원.  
> 자세한 개선 이력은 `backend/tests/version.md` 참조.


---

## 10. 협업 규칙

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

## 11. 팀원

| 역할 | 담당 |
|---|---|
| 팀장 | FastAPI 백엔드, 하이브리드 RAG 라우팅 설계, 인프라 통합 |
| 팀원 A | 데이터 엔지니어링 (문서 전처리 및 DB 적재 파이프라인) |
| 팀원 B | 자동화 파이프라인 (n8n · Slack 연동 워크플로우) |
| 팀원 C | AI 성능 평가 및 논문 작성 (프롬프트 튜닝, 평가 질의셋, KIPS) |
