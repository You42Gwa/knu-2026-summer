from __future__ import annotations

import re

_PANDAS_KEYWORDS = re.compile(
    r"명단|몇\s*명|\d+\s*명|인원|금액|얼마|통계|집계|합계|총\s*금액|지급액|목록|리스트|누가|누구|현황|조회|어느\s*학과|무슨\s*학과|어느\s*반|종목"
    r"|받았어|받았나|있어\?|있나|있는지|수혜자|받은\s*학생|수혜\s*받",
    re.IGNORECASE,
)
_VECTOR_PROCEDURE = re.compile(
    r"방법|절차|기준|서류|자격|안내|규정|내용|제도|신청|문의|어떻게|왜|이유|달라|같아|차이|비교",
    re.IGNORECASE,
)
# "지급\s*금액" 제거: 파일명 기반 총액 추출로 pandas에서 처리
_VECTOR_OVERRIDE = re.compile(
    r"설명해|설명해줘|목적|문서의\s*내용|내용을\s*설명|어떤\s*내용|몇\s*월|몇\s*년|날짜|작성됐|어느\s*학교|총\s*지급액|장학금\s*총액"
    r"|어디서|어느\s*기관|기관명|단체명|출처|발행",
    re.IGNORECASE,
)
_AGG_COUNT = re.compile(r"몇\s*명|총\s*인원|인원은|명이야|명인가|몇명", re.IGNORECASE)
_AGG_SUM   = re.compile(r"총\s*금액|합계금액|얼마야|얼마인|지급\s*금액|장학금\s*총액", re.IGNORECASE)
_AGG_MAX   = re.compile(r"최고|최대|가장\s*(?:많|높|큰)|제일\s*(?:높|많|큰)", re.IGNORECASE)
_AGG_MIN   = re.compile(r"최저|최소|가장\s*(?:적|낮|작은?)|제일\s*(?:낮|적|작)", re.IGNORECASE)
_AGG_PER   = re.compile(r"1인당|한\s*명이\s*받은|학생\s*한\s*명|인당", re.IGNORECASE)


def _route(question: str) -> str:
    if _VECTOR_OVERRIDE.search(question):
        return "VECTOR"
    # _AGG_SUM은 PANDAS에서 소스 파일명 기반 총액 추출로 처리
    if _PANDAS_KEYWORDS.search(question) or _AGG_SUM.search(question):
        if _VECTOR_PROCEDURE.search(question):
            return "VECTOR"
        return "PANDAS"
    if "장학" in question and not _VECTOR_PROCEDURE.search(question):
        return "PANDAS"
    return "VECTOR"
