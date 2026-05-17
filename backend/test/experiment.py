import requests
import pandas as pd
import datetime
import os
import time
import re

# --- [설정] 서버 주소 및 API 키 ---
API_URL = "http://localhost:8080/chat"
API_KEY = os.getenv("API_KEY", "")  # .env에 설정한 API_KEY 입력
SAVE_DIR = "./result"

if not os.path.exists(SAVE_DIR):
    os.makedirs(SAVE_DIR)

eval_data = [
    # ===================================================
    # VECTOR 평가 (비정형 데이터: 키오스크 입찰공고 HWP)
    # ===================================================
    {"no": 1,  "type": "VECTOR", "q": "키오스크 구축 사업의 총 예산은 얼마인가요?", "ans": "55,000천원(부가세포함)"},
    {"no": 2,  "type": "VECTOR", "q": "키오스크는 총 몇 대가 설치되며, 설치 장소는 어디인가요?", "ans": "3대 (본교, 분교, 역사관)"},
    {"no": 3,  "type": "VECTOR", "q": "입찰에 참여하려면 어떤 자격 요건을 갖추어야 하나요?", "ans": "동문(또는 동문 가족·협업)이며 최근 2년 유사 납품실적 보유"},
    {"no": 4,  "type": "VECTOR", "q": "입찰 공고 기간은 언제부터 언제까지인가요?", "ans": "2024. 6. 18(화) ~ 2024. 7. 1(월) 17:00"},
    {"no": 5,  "type": "VECTOR", "q": "입찰 서류 접수는 언제, 어디서 하나요?", "ans": "2024. 7. 5(금) 14:00, 기념사업회 사무국"},
    {"no": 6,  "type": "VECTOR", "q": "제안 발표 심사 날짜와 장소는 어디인가요?", "ans": "2024. 7. 10(수) 13:00, 기념사업회 회의실"},
    {"no": 7,  "type": "VECTOR", "q": "키오스크 화면 크기는 몇 인치로 요구되나요?", "ans": "55인치 UHD 가로 패널"},
    {"no": 8,  "type": "VECTOR", "q": "키오스크를 조작할 때 마우스나 키보드가 필요한가요?", "ans": "불필요 (터치패널 및 모션인식보드로 조작)"},
    {"no": 9,  "type": "VECTOR", "q": "동문검색 소프트웨어에는 몇 기수까지의 명단이 들어가나요?", "ans": "1기~96기 (1930년~2025년 졸업생)"},
    {"no": 10, "type": "VECTOR", "q": "동문 검색 시 어떤 검색 기능이 있어야 하나요?", "ans": "초성 검색 / 개인별·기수별·학과별 검색"},
    {"no": 11, "type": "VECTOR", "q": "동문 검색 화면에 기본적으로 어떤 정보가 표시되어야 하나요?", "ans": "앨범 사진, 이름, 기수, 졸업년도"},
    {"no": 12, "type": "VECTOR", "q": "입찰 신청 시 구비 서류는 어떤 것들이 있나요?", "ans": "입찰신청서, 제안서, 견적서, 자격 증빙서류, 신분증, 서약서"},
    {"no": 13, "type": "VECTOR", "q": "1차 서류심사에서는 무엇을 평가하나요?", "ans": "제안서류 평가 및 견적 금액"},
    {"no": 14, "type": "VECTOR", "q": "제안 발표회에서 발표와 질의응답 시간은 각각 몇 분인가요?", "ans": "발표 10분 이내 / 질의응답 20분 이내"},
    {"no": 15, "type": "VECTOR", "q": "키오스크 S/W 자료의 귀속 권한은 누구에게 있나요?", "ans": "대구공고 개교 100주년 기념사업회"},
    {"no": 16, "type": "VECTOR", "q": "구동 PC의 권장 사양은 어떻게 되나요?", "ans": "i5-12400, 6G RAM, 3세대 SSD"},
    {"no": 17, "type": "VECTOR", "q": "재공모 시 기업 단독 출품일 경우 업체 선정은 어떻게 하나요?", "ans": "협상에 의해 기업 선정"},
    {"no": 18, "type": "VECTOR", "q": "보안확약서에 따라 사업수행 중 얻은 자료는 어떻게 처리해야 하나요?", "ans": "반납 및 파기, 복사본 보유 금지"},
    {"no": 19, "type": "VECTOR", "q": "심사위원회 회의는 투명성을 위해 공개되나요?", "ans": "비공개 원칙"},
    {"no": 20, "type": "VECTOR", "q": "키오스크 설치 시 구조 보강 비용이 발생하면 누가 부담하나요?", "ans": "선정기업"},
    {"no": 21, "type": "VECTOR", "q": "화면 멈춤 현상을 막기 위해 어떤 통신 방식을 사용해야 하나요?", "ans": "RS232C 통신보드 및 통신프로토콜"},
    {"no": 22, "type": "VECTOR", "q": "본 사업의 정확한 계약 주체(기관)는 어디인가요?", "ans": "대구공고 개교 100주년 기념사업회(장학회)"},
    {"no": 23, "type": "VECTOR", "q": "하도급업체가 보안을 위반할 경우 주사업자도 책임을 져야 하나요?", "ans": "동일한 법적 책임"},
    {"no": 24, "type": "VECTOR", "q": "대리인이 참가신청서를 낼 때 추가 서류는 무엇인가요?", "ans": "위임장, 재직증명서, 신분증 사본"},
    {"no": 25, "type": "VECTOR", "q": "키오스크 구축 사업의 기간은 언제까지인가요?", "ans": "계약일 ~ 2024. 11. 30"},
    {"no": 26, "type": "VECTOR", "q": "우선협상자 발표 예정일은 언제인가요?", "ans": "2024. 7. 12(금)"},
    {"no": 27, "type": "VECTOR", "q": "입찰 공고는 어디서 확인할 수 있나요?", "ans": "대구공고 총동문회 홈페이지(www.dgorg.org) 및 밴드"},
    {"no": 28, "type": "VECTOR", "q": "키오스크에서 개인정보 보호 정책을 어떻게 처리해야 하나요?", "ans": "개인정보 보호 정책을 고려하여 정보 제공"},
    {"no": 29, "type": "VECTOR", "q": "구동 PC 형태에 대한 디자인 요구사항은 무엇인가요?", "ans": "Slim 디자인, Smart 소형 일체형 제작"},
    {"no": 30, "type": "VECTOR", "q": "선정된 기업이 수행할 수 없다고 판단될 경우 어떻게 처리하나요?", "ans": "위원회 의결을 거쳐 차선 기업 선정"},
    {"no": 31, "type": "VECTOR", "q": "사업비에는 어떤 항목들이 포함되나요?", "ans": "제품제작, 운반비, 설치비, 부대시설비 등 설치완료까지 일체 비용"},
    {"no": 32, "type": "VECTOR", "q": "입찰 공고의 주최 기관명은 무엇인가요?", "ans": "대구공업공업고등학교 개교100주년 기념사업회"},
    {"no": 33, "type": "VECTOR", "q": "제품 크기(폼팩터)에 대한 제한이 있나요?", "ans": "제한 없음 (단, 55인치 가로 비율 화면 구성 요구)"},
    {"no": 34, "type": "VECTOR", "q": "입찰 참가 신청서 양식은 몇 호인가요?", "ans": "양식 1"},
    {"no": 35, "type": "VECTOR", "q": "보안확약서 양식은 몇 호인가요?", "ans": "양식 2"},
    {"no": 36, "type": "VECTOR", "q": "입찰 관련 문의 전화번호는 무엇인가요?", "ans": "053-957-0551"},
    {"no": 37, "type": "VECTOR", "q": "동문검색 소프트웨어에서 기수별 검색이 지원되어야 하나요?", "ans": "예, 개인별·기수별·학과별 검색 모두 지원 필요"},
    {"no": 38, "type": "VECTOR", "q": "키오스크는 향후 어떤 기능과 연동될 예정인가요?", "ans": "홈페이지 연동"},
    {"no": 39, "type": "VECTOR", "q": "서류심사 통과 기업에게는 어떻게 알려주나요?", "ans": "개별 통지"},
    {"no": 40, "type": "VECTOR", "q": "입찰신청 구비서류 중 견적서 작성 형식은 어떻게 되나요?", "ans": "업체별 서식에 따름"},
    {"no": 41, "type": "VECTOR", "q": "입찰 자격 요건에서 동문이 아닌 경우 어떤 조건으로 참여할 수 있나요?", "ans": "동문 가족이거나 동문과 협업 가능한 경우"},
    {"no": 42, "type": "VECTOR", "q": "PC 조작 방법(터치) 사양은 어떻게 명시되어 있나요?", "ans": "55인치 투명 터치패널 및 정전·IR 인식보드"},
    {"no": 43, "type": "VECTOR", "q": "보안확약서 위반 시 어떤 제재를 받을 수 있나요?", "ans": "사업 참여 제한 또는 관련 법규에 따른 책임과 손해배상"},
    {"no": 44, "type": "VECTOR", "q": "선정기업의 S/W 소스와 콘텐츠 자료는 어디에 귀속되나요?", "ans": "대구공고 개교 100주년 기념사업회"},
    {"no": 45, "type": "VECTOR", "q": "적합 기업이 없다고 결정될 경우 어떻게 진행하나요?", "ans": "재입찰(재공모) 실시"},
    {"no": 46, "type": "VECTOR", "q": "1기 졸업생은 몇 년도 졸업생인가요?", "ans": "1930년"},
    {"no": 47, "type": "VECTOR", "q": "96기 졸업생은 몇 년도 졸업생인가요?", "ans": "2025년"},
    {"no": 48, "type": "VECTOR", "q": "입찰공고 번호 형식은 어떻게 되나요?", "ans": "대구공고 개교 100주년 기념사업 제2024- 호"},
    {"no": 49, "type": "VECTOR", "q": "선정기업은 사업회의 요구에 따라 무엇을 해야 하나요?", "ans": "관련 자료 제출 및 원활한 수행을 위한 협조"},
    {"no": 50, "type": "VECTOR", "q": "본 입찰은 어떤 방식의 계약(구매)인가요?", "ans": "용역 구매 입찰"},
 
    # ===================================================
    # SQL 평가 (정형 데이터: 2025 기부금 내역 XLSX)
    # ===================================================
    {"no": 51, "type": "SQL", "q": "기부금 후원 명단에 등록된 총 건수(행수)는 몇 건이야?", "ans": "158건"},
    {"no": 52, "type": "SQL", "q": "기부금 후원 내역의 총 모금액(출연금액 합계)은 얼마야?", "ans": "977,070,000원"},
    {"no": 53, "type": "SQL", "q": "49기 동문들이 기부한 건수는 총 몇 건이야?", "ans": "41건"},
    {"no": 54, "type": "SQL", "q": "단건 기준으로 가장 많은 금액을 기부한 사람은 누구고, 얼마야?", "ans": "노*찬, 225,000,000원"},
    {"no": 55, "type": "SQL", "q": "'이*곤' 동문이 기부한 총 금액은 얼마야?", "ans": "40,000,000원"},
    {"no": 56, "type": "SQL", "q": "2025년 2월 21일에 기부한 건수는 총 몇 건이야?", "ans": "25건"},
    {"no": 57, "type": "SQL", "q": "55기 이*규 동문의 기부 총 건수는 몇 번이야?", "ans": "8번"},
    {"no": 58, "type": "SQL", "q": "기부 금액이 10,000,000원 이상인 건수는 총 몇 건이야?", "ans": "18건"},
    {"no": 59, "type": "SQL", "q": "자동차과동문회의 기부 횟수와 총액은 얼마야?", "ans": "2회, 3,000,000원"},
    {"no": 60, "type": "SQL", "q": "46기 이*삼 동문의 총 기부 금액은 얼마야?", "ans": "23,000,000원"},
    {"no": 61, "type": "SQL", "q": "기부 금액이 가장 적은 건의 기부자 이름과 금액은?", "ans": "이*구, 20,000원"},
    {"no": 62, "type": "SQL", "q": "2025년 1월에 기부된 금액의 총합은 얼마야?", "ans": "43,600,000원"},
    {"no": 63, "type": "SQL", "q": "기부 금액이 정확히 200,000원인 건수는 총 몇 건이야?", "ans": "24건"},
    {"no": 64, "type": "SQL", "q": "2025-008 발행번호로 등록된 기부 내역은 총 몇 건이야?", "ans": "8건"},
    {"no": 65, "type": "SQL", "q": "60기 동문 중 가장 큰 금액을 기부한 사람은 누구고, 얼마야?", "ans": "오*환, 3,000,000원"},
    {"no": 66, "type": "SQL", "q": "2025년 3월에 접수된 건 중 가장 큰 기부금은 얼마이고, 기부자는?", "ans": "225,000,000원, 노*찬"},
    {"no": 67, "type": "SQL", "q": "송**랑 동문이 기부한 금액과 기수는?", "ans": "100,000,000원, 37기"},
    {"no": 68, "type": "SQL", "q": "56기 동문들이 기부한 총 건수는 몇 번이야?", "ans": "18번"},
    {"no": 69, "type": "SQL", "q": "49기 동문들의 기부금 총액은 얼마야?", "ans": "27,000,000원"},
    {"no": 70, "type": "SQL", "q": "개인별 기부 합산 기준 상위 3명의 이름과 금액을 알려줘.", "ans": "노*찬(225,000,000원), 송**랑(100,000,000원), (주)*산(100,000,000원)"},
    {"no": 71, "type": "SQL", "q": "(주)*산 기업에서 기부한 금액은 얼마야?", "ans": "100,000,000원"},
    {"no": 72, "type": "SQL", "q": "발행번호가 2025-100 이후(초과)인 건수는 몇 건이야?", "ans": "46건"},
    {"no": 73, "type": "SQL", "q": "58기 중 이름이 '김'으로 시작하는 사람은 누구야?", "ans": "김*담, 김*수"},
    {"no": 74, "type": "SQL", "q": "출연일자가 2025년 5월인 기부 내역은 총 몇 건이야?", "ans": "7건"},
    {"no": 75, "type": "SQL", "q": "1,000,000원을 기부한 내역은 총 몇 건이야?", "ans": "55건"},
    {"no": 76, "type": "SQL", "q": "2025년 2월의 총 기부금액은 얼마야?", "ans": "209,900,000원"},
    {"no": 77, "type": "SQL", "q": "2025년 3월의 총 기부금액은 얼마야?", "ans": "580,450,000원"},
    {"no": 78, "type": "SQL", "q": "43기 동문들의 기부금 총액은 얼마야?", "ans": "63,000,000원"},
    {"no": 79, "type": "SQL", "q": "51기 동문들의 기부금 총액은 얼마야?", "ans": "58,500,000원"},
    {"no": 80, "type": "SQL", "q": "2025년 3월 10일에 기부한 사람들의 이름과 금액을 알려줘.", "ans": "윤*동(70,000,000원), 노*찬(225,000,000원), 시*성(500,000원), 박*현(500,000원), 오*철(500,000원), 도*원(500,000원)"},
    {"no": 81, "type": "SQL", "q": "기부 건수가 가장 많은 기수는 몇 기이고 몇 건이야?", "ans": "49기, 41건"},
    {"no": 82, "type": "SQL", "q": "500,000원을 기부한 건수는 총 몇 건이야?", "ans": "23건"},
    {"no": 83, "type": "SQL", "q": "63기 동문들의 기부 내역(이름, 금액)은?", "ans": "63회 동기회 토목과(3,000,000원), 63회 동기회(5,000,000원)"},
    {"no": 84, "type": "SQL", "q": "2번 이상 기부한 사람은 몇 명이야?", "ans": "13명(단체 포함)"},
    {"no": 85, "type": "SQL", "q": "현대중공업대공동문회의 기부 금액은 얼마야?", "ans": "1,000,000원"},
    {"no": 86, "type": "SQL", "q": "기부금 총액 기준으로 가장 많이 낸 기수는 몇 기야?", "ans": "33기 (225,000,000원, 노*찬)"},
    {"no": 87, "type": "SQL", "q": "2025년 4월의 총 기부금액은 얼마야?", "ans": "88,550,000원"},
    {"no": 88, "type": "SQL", "q": "100,000원을 기부한 건수는 총 몇 건이야?", "ans": "6건"},
    {"no": 89, "type": "SQL", "q": "52기 동문들의 기부금 총액은 얼마야?", "ans": "8,500,000원"},
    {"no": 90, "type": "SQL", "q": "5,000,000원을 기부한 건수는 총 몇 건이야?", "ans": "6건"},
    {"no": 91, "type": "SQL", "q": "2025-02-27에 기부한 사람들의 이름과 금액은?", "ans": "김*수(1,000,000원), 조*래(5,000,000원), 자동차과동문회(2,000,000원), 자동차과동문회(1,000,000원)"},
    {"no": 92, "type": "SQL", "q": "37기 동문들의 기부금 총액은 얼마야?", "ans": "112,000,000원"},
    {"no": 93, "type": "SQL", "q": "기부금이 50,000원 이하인 내역은 몇 건이야?", "ans": "3건 (차*권 50,000원, 윤*노 50,000원, 이*구 20,000원)"},
    {"no": 94, "type": "SQL", "q": "3,000,000원을 기부한 건수는 총 몇 건이야?", "ans": "13건"},
    {"no": 95, "type": "SQL", "q": "건축과동문회의 기부 금액은 얼마야?", "ans": "4,550,000원"},
    {"no": 96, "type": "SQL", "q": "44기 동문들의 기부 건수와 총액은?", "ans": "4건, 14,500,000원"},
    {"no": 97, "type": "SQL", "q": "56기 동문들의 기부금 총액은 얼마야?", "ans": "16,220,000원"},
    {"no": 98, "type": "SQL", "q": "이*규라는 이름을 가진 기부자들의 총 기부 건수는?", "ans": "이*규(55기) 8건 + 이*규(49기) 1건 = 9건"},
    {"no": 99, "type": "SQL", "q": "2025년 4월의 기부 건수는 몇 건이야?", "ans": "9건"},
    {"no": 100, "type": "SQL", "q": "기부금 내역에서 기수 정보가 없는(단체 등) 건수는 몇 건이야?", "ans": "6건 (현대중공업대공동문회, (주)*산, 자동차과동문회×2, (주)금**, 건축과동문회)"},
]

# ── 1. 한글 숫자 → 아라비아 숫자 변환 ──────────────────────────────────────

DIGIT_MAP = {
    '일': 1, '이': 2, '삼': 3, '사': 4, '오': 5,
    '육': 6, '칠': 7, '팔': 8, '구': 9,
}
UNIT_MAP = {
    '십': 10, '백': 100, '천': 1_000,
    '만': 10_000, '억': 100_000_000, '조': 1_000_000_000_000,
}

def korean_to_number(text: str) -> str:
    """
    한글로 표기된 숫자를 아라비아 숫자로 변환.

    지원 패턴 예시:
        이백이십오억 원          → 22500000000
        삼천만                   → 30000000
        일억 이천오백만          → 125000000
        225,000,000              → (그대로)
        2억 2500만               → 225000000
        오십오                   → 55
        열둘  (십이 방언) 미지원  → (그대로, 추후 확장 가능)
    """

    def parse_chunk(s: str) -> int:
        """
        '만' 미만 단위 청크(일~천 범위) 파싱.
        예: '이백이십오' → 225,  '오백' → 500,  '삼' → 3
        """
        result = 0
        current = 0  # 현재 자릿수 앞에 붙는 숫자
        i = 0
        while i < len(s):
            ch = s[i]
            if ch in DIGIT_MAP:
                current = DIGIT_MAP[ch]
                i += 1
            elif ch in ('십', '백', '천'):
                unit = UNIT_MAP[ch]
                # '백' 앞에 숫자 없으면 1로 처리 (예: '백' = 100)
                result += (current if current else 1) * unit
                current = 0
                i += 1
            else:
                i += 1  # 알 수 없는 문자 skip
        result += current  # 마지막 낱자 (예: '삼' → 3)
        return result

    def parse_korean_number(s: str) -> int:
        """
        만/억/조 단위를 포함한 전체 숫자 파싱.
        예: '이억이천오백만' → 225_000_000
        """
        # 조/억/만 기준으로 분리
        BIG_UNITS = [('조', 1_000_000_000_000),
                     ('억', 100_000_000),
                     ('만', 10_000)]
        total = 0
        remaining = s

        for unit_char, unit_val in BIG_UNITS:
            if unit_char in remaining:
                left, remaining = remaining.split(unit_char, 1)
                chunk_val = parse_chunk(left) if left else 1
                total += chunk_val * unit_val

        # 남은 부분: 만 미만 (천/백/십/일 단위)
        if remaining:
            total += parse_chunk(remaining)

        return total

    # ── 한글+아라비아 혼합 패턴: "2억 2500만", "1억2천만" 등 ──────────────
    def replace_mixed(m: re.Match) -> str:
        """아라비아 + 한글 단위 혼합 표현 처리"""
        s = m.group(0)
        BIG_UNITS = [('조', 1_000_000_000_000),
                     ('억', 100_000_000),
                     ('만', 10_000)]
        total = 0
        remaining = s

        for unit_char, unit_val in BIG_UNITS:
            pat = r'(\d[\d,]*)' + unit_char
            um = re.search(pat, remaining)
            if um:
                val = int(um.group(1).replace(',', ''))
                total += val * unit_val
                # 매칭된 부분 제거
                remaining = remaining[:um.start()] + remaining[um.end():]

        # 남은 순수 숫자 (있으면)
        leftover = re.search(r'\d[\d,]*', remaining)
        if leftover:
            total += int(leftover.group(0).replace(',', ''))

        return str(total)

    # ── 한글 전용 숫자 패턴 ──────────────────────────────────────────────────
    KR_NUM_CHARS = '일이삼사오육칠팔구십백천만억조'
    kr_pattern = re.compile(
        rf'[{KR_NUM_CHARS}]+'
        rf'(?:\s*[{KR_NUM_CHARS}]+)*'
    )

    # 아라비아+한글 혼합 먼저 처리 (우선순위 높음)
    mixed_pattern = re.compile(
        r'(?:\d[\d,]*\s*억)(?:\s*\d[\d,]*\s*만)?(?:\s*\d[\d,]*)?'
        r'|(?:\d[\d,]*\s*만)(?:\s*\d[\d,]*)?'
        r'|\d[\d,]*\s*조'
    )
    text = mixed_pattern.sub(replace_mixed, text)

    # 순수 한글 숫자 처리
    def replace_kr(m: re.Match) -> str:
        raw = m.group(0).replace(' ', '')
        val = parse_korean_number(raw)
        return str(val) if val > 0 else m.group(0)

    text = kr_pattern.sub(replace_kr, text)
    return text


def normalize(text: str) -> str:
    """숫자·단위·공백을 정규화해서 비교 가능한 형태로 변환"""
    text = korean_to_number(text)
    text = str(text).strip()
    # 천 단위 쉼표 제거: 977,070,000 → 977070000
    text = re.sub(r'(\d),(\d)', r'\1\2', text)
    # 단위 제거
    text = re.sub(r'[원건명회번기회차분]', '', text)
    # 공백 정규화
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def extract_key_tokens(ans: str) -> list[str]:
    """
    정답 문자열에서 핵심 토큰(숫자, 이름, 키워드) 목록을 추출.
    콤마 구분 복합 정답을 모두 개별 토큰으로 분리.
    """
    norm = normalize(ans)
    tokens = []

    # 1) 순수 숫자 추출 (금액·건수 등)
    numbers = re.findall(r'\d+', norm)
    tokens.extend(numbers)

    # 2) 한글 이름/단어 추출 (2글자 이상)
    words = re.findall(r'[가-힣]{2,}', norm)
    tokens.extend(words)

    # 3) 영숫자 혼합 토큰 (예: RS232C, i5-12400)
    alnum = re.findall(r'[A-Za-z0-9][A-Za-z0-9\-]+', norm)
    tokens.extend(alnum)

    return list(dict.fromkeys(tokens))  # 순서 유지 중복 제거


def check_answer(ans: str, ai_ans: str) -> str:
    """
    O  : 핵심 토큰이 모두 ai_ans에 포함
    △  : 일부 토큰만 포함 (부분 정답)
    X  : 핵심 토큰이 하나도 없음
    """
    norm_ai = normalize(ai_ans)
    tokens = extract_key_tokens(ans)

    if not tokens:
        return "△"

    matched = [t for t in tokens if t in norm_ai]
    ratio = len(matched) / len(tokens)

    if ratio == 1.0:
        return "O"
    elif ratio >= 0.5:   # 절반 이상 맞으면 부분 정답
        return "△"
    else:
        return "X"


def run_experiment():
    print("\n" + "="*50)
    print(" 🚀 100문항 자동 평가 시작")
    print("="*50 + "\n")

    results = []
    headers = {"X-API-Key": API_KEY}
    
    total_count = len(eval_data)
    route_correct_count = 0

    for item in eval_data:
        # if(item['no'] <= 50):
        #     continue
        print(f"[{item['no']}/{total_count}] 질문 처리 중: {item['q']}")
        
        start_time = time.time()
        try:
            # 1. API 호출
            resp = requests.post(API_URL, json={"question": item['q']}, headers=headers, timeout=60)
            elapsed_time = round(time.time() - start_time, 2)

            if resp.status_code == 200:
                data = resp.json()
                ai_ans = data.get("answer", "")
                pred_route = data.get("source", "").upper() # "vector" -> "VECTOR"
                
                # 2. 라우팅 정확도 체크
                is_route_ok = "O" if item['type'] == pred_route else "X"
                if is_route_ok == "O":
                    route_correct_count += 1
                
                # 3. 답변 내 정답 포함 여부 (단순 텍스트 매칭)
                # 정답 숫자나 핵심 단어가 AI 답변에 들어있는지 확인
                ans_check = check_answer(item['ans'], ai_ans)
                
            else:
                ai_ans, pred_route, is_route_ok, ans_check, elapsed_time = f"Error: {resp.status_code}", "ERR", "X", "X", 0

        except Exception as e:
            elapsed_time = round(time.time() - start_time, 2)  # 실제 소요시간 기록
            ai_ans = f"Exception: {str(e)}"
            pred_route, is_route_ok, ans_check = "ERR", "X", "X"
            
        # [실시간 출력] 개별 문항 결과 상세화
        print(f"[라우팅] 목표: {item['type']} \n[라우팅] 결과: {pred_route} \n[라우팅] -> [{is_route_ok}]")
        print(f"[답변] 목표: {item['ans']} \n[답변] 결과: {ai_ans} \n[답변] -> [{ans_check}]")
        print(f"[시간] {elapsed_time}s")
        print("-" * 30)

        # 결과 저장
        results.append({
            "No": item['no'],
            "유형": item['type'],
            "질문": item['q'],
            "목표 정답": item['ans'],
            "AI 판단 라우팅": pred_route,
            "라우팅 성공": is_route_ok,
            "AI 최종 답변": ai_ans,
            "정답 포함여부(참고)": ans_check,
            "소요시간(초)": elapsed_time
        })

    # --- [결과 분석 및 저장] ---
    df = pd.DataFrame(results)
    
    # 통계 계산
    route_acc = (route_correct_count / total_count) * 100
    
    now_str = datetime.datetime.now().strftime("%m%d_%H%M")
    file_name = f"실험결과_{now_str}_ACC_{route_acc:.1f}.xlsx"
    save_path = os.path.join(SAVE_DIR, file_name)
    
    # 엑셀 저장
    df.to_excel(save_path, index=False)
    
    print("\n" + "="*50)
    print(f"🎉 평가 완료! (라우팅 정확도: {route_acc:.1f}%)")
    print(f"📂 결과 파일: {save_path}")
    print("="*50)

def question():
    print("\n" + "="*50)
    print(" 🚀 개별 질문 평가 시작")
    print("="*50 + "\n")

    headers = {"X-API-Key": API_KEY}
    type = input("질문의 유형(SQL or VECTOR // Q = 종료) : ")
    ques = input("질문의 내용 : ")
    ans = input("목표 정답(빈칸 가능) : ")
    
    while(type != "Q"):
        
        start_time = time.time()
        try:
            # 1. API 호출
            resp = requests.post(API_URL, json={"question": ques}, headers=headers, timeout=60)
            elapsed_time = round(time.time() - start_time, 2)

            if resp.status_code == 200:
                data = resp.json()
                ai_ans = data.get("answer", "")
                pred_route = data.get("source", "").upper() # "vector" -> "VECTOR"
                
                # 2. 라우팅 정확도 체크
                is_route_ok = "O" if type == pred_route else "X"
                
                # 3. 답변 내 정답 포함 여부 (단순 텍스트 매칭)
                # 정답 숫자나 핵심 단어가 AI 답변에 들어있는지 확인
                ans_check = "O" if str(ans).split(',')[0].replace("원","").replace("건","").strip() in ai_ans else "△"
                
            else:
                ai_ans, pred_route, is_route_ok, ans_check, elapsed_time = f"Error: {resp.status_code}", "ERR", "X", "X", 0

        except Exception as e:
            elapsed_time = round(time.time() - start_time, 2)  # 실제 소요시간 기록
            ai_ans = f"Exception: {str(e)}"
            pred_route, is_route_ok, ans_check = "ERR", "X", "X"
            
        # [실시간 출력] 개별 문항 결과 상세화
        print(f"[라우팅] 목표: {type} \n[라우팅] 결과: {pred_route} \n[라우팅] -> [{is_route_ok}]")
        print(f"[답변] 목표: {ans} \n[답변] 결과: {ai_ans} \n[답변] -> [{ans_check}]")
        print(f"[시간] {elapsed_time}s")
        print("-" * 30)
        
        type = input("질문의 유형(SQL or VECTOR) : ")
        ques = input("질문의 내용 : ")
        ans = input("목표 정답(빈칸 가능) : ")



if __name__ == "__main__":
    what = input("입력 1 or 2 (test set : 1 / 개별 질문 : 2) : ")
    if what == "1":
        run_experiment()
    elif what =="2":
        question()