# AI 기반 조직 문서 통합 조회 및 의사결정 지원 시스템


##  1. 프로젝트 개요
*  **과제명**: AI 기반 조직 문서 통합 조회 및 의사결정 지원 자동화 시스템 개발 및 연구 
*  **추진 배경**: 조직 내 다수의 문서(Google Docs, Sheets)가 분산되어 있어 정보 활용의 비효율이 발생하며, 이를 개선하기 위한 데이터 기반 자동화 기능 도입이 필요함 
*  **최종 목표**: 사용자의 질의를 분석하여 대상 문서를 스스로 결정하고, 수집된 데이터를 바탕으로 근거를 포함한 답변을 생성하는 고신뢰도 AI 에이전트 구축 

---

##  2. 기술 스택

### **Languages & Frameworks**
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi)
![LangChain](https://img.shields.io/badge/LangChain-1C3C3C?style=for-the-badge&logo=langchain&logoColor=white)

### **Automation & Infrastructure**
![n8n](https://img.shields.io/badge/n8n-FF6D5A?style=for-the-badge&logo=n8n&logoColor=white)
![Oracle](https://img.shields.io/badge/Oracle-F80000?style=for-the-badge&logo=oracle&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)

### **Tools & Interface**
![GitHub](https://img.shields.io/badge/github-%23121011.svg?style=for-the-badge&logo=github&logoColor=white)
![Slack](https://img.shields.io/badge/Slack-4A154B?style=for-the-badge&logo=slack&logoColor=white)
![Google Sheets](https://img.shields.io/badge/Google%20Sheets-34A853?style=for-the-badge&logo=google-sheets&logoColor=white)
---

##  3. 시스템 아키텍처 (Architecture)

 본 시스템은 **2단계 LLM 추론 로직**을 통해 정확도와 비용 효율성을 극대화



1.  **1차 LLM (Initial Analysis)**: 사용자 질의 분석 후 대상 파일 및 필요한 데이터 범위(시트/열/섹션) 결정 
2.  **Data Retrieval**: n8n이 결정된 범위 내의 데이터만 선택적으로 수집 및 정제 
3.  **2차 LLM (Response Generation)**: 수집된 데이터와 규칙을 대조하여 요약/비교 수행 및 근거 매핑 

---

##  4. 폴더 구조 (Project Structure)

```text
├── src/
│   ├── api/              # FastAPI 기반 커스텀 백엔드
│   ├── agent/            # 1·2차 LLM 추론 로직 (LangChain)
│   ├── utils/            # 데이터 전처리 및 검증 모듈
│   └── workflow/         # n8n 워크플로우 백업 (.json)
├── configs/              # API 설정 및 메타데이터
├── docs/                 # 기획서 및 학술대회 논문 초안
├── requirements.txt      # 의존성 패키지 목록
└── README.md             # 프로젝트 메인 설명서
```

##  5. 협업
### Git
- **Branch**: main (배포용) ← develop (통합 테스트) ← feature/기능명 (개별 개발)

- **Commit Message 규칙**:
    - `Feat`: 새로운 기능 추가

    - `Fix`: 버그 수정

    - `Docs`: 문서 수정 (README 등)

    - `Refactor`: 코드 리팩토링 (기능 변경 없는 코드 구조 변경)

### n8n
- **버전 관리**: n8n 워크플로우 수정 후 반드시 JSON 파일로 내보내어 src/workflow/ 폴더에 커밋하여 코드와 싱크를 맞춤

##  6. 시작하기


### 1. 가상환경 생성 및 활성화

```PowerShell
python -m venv .venv
.venv\Scripts\activate
```
### 2. 필수 라이브러리 설치

```PowerShell
pip install -r requirements.txt
```
생성된 .env 파일을 열어 각자 발급받은 API Key 입력


##  7. 정량적 성과 목표
학술대회 논문 투고 및 실무 역량 증명을 위해 다음 지표를 추적하고 기록합니다.

- **신뢰성**: AI 답변이 제공된 문서 근거 내에서만 작성되었는지 측정 (목표: 95% 이상)

- **추출 정확도**: 답변의 출처로 제시한 파일/행 번호의 실제 일치율 (목표: 90% 이상)

- **효율성**: 기존 수동 문서 탐색 대비 응답 시간 단축률 (목표: 90% 이상)

- **비용 최적화**: 1차 분석을 통한 범위 제한 수집으로 인한 토큰 사용량 절감 수치 제시

## 8. 팀원 소개
팀장: 

팀원 A: 

팀원 B: 

팀원 C: 