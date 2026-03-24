# AI 기반 장학재단 내부 문서 통합 조회 및 업무 자동화 에이전트

## 1. 프로젝트 개요
* **과제명**: 폐쇄망 환경을 위한 하이브리드 RAG 기반 사내 문서 처리 및 의사결정 지원 시스템 구축
* **추진 배경**: 장학재단 등 보안이 민감한 기관은 내부 문서(예산안, 규정집, 영수증 등) 처리에 상용 클라우드 LLM 도입이 불가능함. 또한 기존 텍스트 위주의 단순 RAG 시스템은 예산 비율 계산 등 정형 데이터 기반의 복합 수치 연산에서 심각한 할루시네이션(환각)을 유발함.
* **최종 목표**: 오픈소스 로컬 LLM과 하이브리드 데이터베이스(RDBMS+VDBMS) 라우팅 기술을 결합하여, 사내망에서 안전하게 동작하며 수치 연산 오류를 최소화한 Slack 기반 자동화 챗봇 에이전트 구축.

---

## 2. 기술 스택

### **Languages & Frameworks**
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi)
![LangChain](https://img.shields.io/badge/LangChain-1C3C3C?style=for-the-badge&logo=langchain&logoColor=white)

### **AI & Database (Hybrid Storage)**
![Ollama](https://img.shields.io/badge/Ollama-000000?style=for-the-badge&logo=ollama&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![ChromaDB](https://img.shields.io/badge/ChromaDB-FF6D5A?style=for-the-badge&logo=chroma&logoColor=white)

### **Automation & Infrastructure**
![n8n](https://img.shields.io/badge/n8n-FF6D5A?style=for-the-badge&logo=n8n&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)

### **Tools & Interface**
![GitHub](https://img.shields.io/badge/github-%23121011.svg?style=for-the-badge&logo=github&logoColor=white)
![Slack](https://img.shields.io/badge/Slack-4A154B?style=for-the-badge&logo=slack&logoColor=white)

---

## 3. 시스템 아키텍처 (Architecture)

본 시스템은 정형 데이터와 비정형 데이터를 분리하여 처리하는 **하이브리드 라우팅 아키텍처**를 통해 답변의 정확도를 극대화합니다.

1. **질의 수신 및 자동화 (n8n)**: Slack Webhook을 통해 사용자의 질문을 수신하고 FastAPI 서버로 전달.
2. **의도 분석 및 Agentic Routing (LangChain)**: 사용자 질의를 분석하여 대상 문서를 결정.
   - **수치 연산/정형 데이터 필요 시**: Text2SQL을 수행하여 PostgreSQL(예산안 xlsx 데이터 등) 조회.
   - **규정/비정형 데이터 필요 시**: 벡터 임베딩을 통해 ChromaDB(규정집 docx, hwp 등) 조회.
3. **응답 및 문서 생성 (Local LLM)**: 수집된 컨텍스트를 종합하여 로컬 LLM(Ollama 구동)이 할루시네이션 없는 답변을 생성하고, 필요시 자동화 보고서를 작성하여 Slack으로 회신.

---

## 4. 폴더 구조 (Project Structure)

```text
├── backend/
│   ├── main.py             # FastAPI 기반 하이브리드 RAG 메인 서버
│   ├── database.py         # PostgreSQL(정형) 및 ChromaDB(비정형) 연결 모듈
│   ├── routers/            # n8n 및 외부 API 통신 라우터
│   ├── data/               # [Git Ignored] 민감한 로컬 내부 문서(hwp, pdf, xlsx) 저장소 
│   └── utils/
│       └── ingest.py       # hwp/pdf를 HTML로 변환하여 표는 DB, 글은 Vector DB로 찢는 하이브리드 파서
├── configs/                # 시스템 설정 및 프롬프트 템플릿
├── docker-compose.yml      # 로컬 인프라(PostgreSQL, ChromaDB, n8n, Ollama) 일괄 실행
├── docs/                   # 정보처리학회(KIPS) 논문 초안 및 구조도
├── requirements.txt        # 의존성 패키지 목록
└── README.md               # 프로젝트 메인 설명서
```
## 5. 협업 규칙
### Git
- Branch: main (배포용) ← develop (통합 테스트) ← feature/기능명 (개별 개발)

- Commit Message 규칙:

    - **Feat**: 새로운 기능 추가 (예: DB 라우팅 로직 추가)
    - **Fix**: 버그 수정 (예: Text2SQL 파싱 오류 해결)
    - **Docs**: 문서 수정 (README, 논문 자료 등)
    - **Refactor**: 코드 리팩토링 (기능 변경 없는 코드 구조 개선)

### n8n
**버전 관리**: n8n 워크플로우 수정 후 반드시 JSON 파일로 내보내어 코드 저장소에 커밋하여 팀원 간 싱크를 맞춤.

## 6. 시작하기
1. 인프라 환경 실행 (Docker)
Docker가 설치된 환경에서 아래 명령어를 통해 데이터베이스와 워크플로우 툴을 실행합니다.
```bash
docker-compose up -d
```

2. 백엔드 가상환경 세팅 및 실행
```bash
cd backend
python -m venv venv
# Windows: venv\Scripts\activate
# Mac/Linux: source venv/bin/activate

pip install -r requirements.txt
uvicorn main:app --reload
```

## 7. 정량적 성과 목표
학술대회(KIPS) 논문 투고 및 실무 적용성 증명을 위해 다음 지표를 추적하고 기록합니다.

- 복합 수치 연산 정답률: 단순 텍스트 검색(Naive RAG) 대조군 대비, 예산 잔액 및 비율 계산 등 정형 데이터 질의 정답률 비교 (목표: 90% 이상 도달)

- 할루시네이션(환각) 감소율: 로컬 LLM의 컨텍스트 압축 및 하이브리드 검색을 통한 환각 발생 빈도 측정

- 파이프라인 자동화 효율성: 질의 입력부터 문서 생성 및 Slack 회신까지의 End-to-End 소요 시간(Latency) 측정 및 기존 수동 작업 대비 단축률 산출

## 8. 팀원 소개
팀장: 백엔드(FastAPI) 구축, 하이브리드 RAG 라우팅 설계 및 인프라 통합

팀원 A: 데이터 엔지니어링 (장학재단 xlsx, docx 문서 전처리 및 DB 적재 파이프라인 구축)

팀원 B: 자동화 파이프라인 (n8n - Slack 연동 워크플로우 설계 및 테스트)

팀원 C: AI 성능 평가 및 논문 작성 (프롬프트 튜닝, 평가 질의셋 구축, KIPS 논문 데이터 추출)