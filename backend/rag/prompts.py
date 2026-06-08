from __future__ import annotations

from langchain_core.prompts import PromptTemplate

MULTI_QUERY_PROMPT = PromptTemplate(
    input_variables=["question"],
    template="""\
사용자의 질문을 서로 다른 표현으로 3가지 재구성하세요. 한국어로 작성하고 한 줄에 하나씩 쓰세요.

원래 질문: {question}
재구성된 질문:""",
)

_RAG_TEMPLATE = """\
당신은 한국어 문서를 분석하는 전문 AI 어시스턴트입니다.
아래 참고 문서를 바탕으로 질문에 정확하고 상세하게 한국어로 답변하세요.

규칙:
- 참고 문서에 있는 표현과 키워드를 최대한 그대로 사용하세요. 임의로 바꾸지 마세요.
- 문서에 구체적인 수치, 명칭, 기준이 있으면 반드시 포함하세요.
- 참고 문서에 없는 내용은 "해당 내용은 문서에서 확인할 수 없습니다."라고 답하세요.

참고 문서:
{context}

질문: {question}
답변:"""

# 문서 설명 전용 템플릿: 문서명·금액·항목에서 목적·내용을 추론하도록 유도
_DOC_EXPLAIN_RAG_TEMPLATE = """\
당신은 한국어 문서를 분석하는 AI 어시스턴트입니다.
아래 참고 문서(특히 [문서 개요] 섹션)를 바탕으로 질문에 답변하세요.

규칙:
- [문서 개요]에 있는 목적, 선발 기준, 지급 기관, 지원 금액 등을 구체적으로 서술하세요.
- 문서에 나온 표현(예: "차등 선발", "성취 동기 부여" 등)을 그대로 사용하세요. 바꾸지 마세요.
- 문서명, 지원 금액, 대상 기관·학생, 포함 항목 등 알 수 있는 정보를 모두 포함하세요.
- 참고 문서에 문서명이라도 있으면 그것을 근거로 설명해 주세요.

참고 문서:
{context}

질문: {question}
답변:"""

_PANDAS_GEN_TEMPLATE = """\
당신은 pandas 전문가입니다. 아래 스키마와 힌트를 보고 질문에 답하는 Python 코드를 작성하세요.
import 없이 변수명(df0, df1 ...)을 바로 사용하세요. 최종 결과는 반드시 result 변수에 저장하세요.
마크다운 코드 블록 없이 순수 Python 코드만 출력하세요.

★ 핵심 규칙:
- "데이터 위치 힌트"에 여러 옵션이 있으면 파일명을 보고 질문과 가장 관련 있는 DataFrame을 선택하세요.
- 힌트가 없을 때는 스키마의 파일명을 참고해 질문에 맞는 DataFrame을 직접 선택하세요.
- 컬럼명은 반드시 스키마의 "컬럼(이 이름만 사용):" 줄에서 가져오세요. 없는 컬럼명은 만들지 마세요.
- "실제값:" 줄은 데이터 값이지 컬럼명이 아닙니다.

코딩 규칙:
1. 텍스트 검색: df['컬럼명'].str.contains('값', na=False)
2. 인원수: result = int(len(filtered_df))
3. 금액 합계: result = float(df['컬럼명'].sum())
4. 명단 조회: result = filtered_df.to_dict('records')
5. 여러 DataFrame 합치기: pd.concat([df0, df1], ignore_index=True)
6. 대소문자 무시: str.contains('값', case=False, na=False)
7. 숫자 비교: df['컬럼명'].astype(float) >= 값

데이터프레임 스키마:
{schema}

{hints}
질문: {question}
코드:"""

RAG_PROMPT = PromptTemplate.from_template(_RAG_TEMPLATE)
DOC_EXPLAIN_RAG_PROMPT = PromptTemplate.from_template(_DOC_EXPLAIN_RAG_TEMPLATE)
