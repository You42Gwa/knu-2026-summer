import requests
import pandas as pd
import datetime
import os
import time

# --- [설정] 서버 주소 및 API 키 ---
API_URL = "http://localhost:8080/chat"
API_KEY = os.getenv("API_KEY", "")  # .env에 설정한 API_KEY 입력
SAVE_DIR = "./result"

if not os.path.exists(SAVE_DIR):
    os.makedirs(SAVE_DIR)

# --- [데이터셋] 50문항 (앞서 확정한 정답 포함) ---
eval_data = [
        # --- VECTOR 평가 (비정형 데이터) ---
        {"no": 1, "type": "VECTOR", "q": "키오스크 구축 사업의 총 예산은 얼마인가요?", "ans": "55,000천원"},
        {"no": 2, "type": "VECTOR", "q": "키오스크는 총 몇 대가 설치되며, 설치 장소는 어디인가요?", "ans": "3대 (본교, 분교, 역사관)"},
        {"no": 3, "type": "VECTOR", "q": "입찰에 참여하려면 어떤 자격 요건을 갖추어야 하나요?", "ans": "동문 등 최근 2년 실적"},
        {"no": 4, "type": "VECTOR", "q": "입찰 공고 기간은 언제부터 언제까지인가요?", "ans": "6. 18 ~ 7. 1"},
        {"no": 5, "type": "VECTOR", "q": "입찰 서류 접수는 언제, 어디서 하나요?", "ans": "7. 5 14:00, 사무국"},
        {"no": 6, "type": "VECTOR", "q": "제안 발표 심사 날짜와 장소는 어디인가요?", "ans": "7. 10 13:00, 회의실"},
        {"no": 7, "type": "VECTOR", "q": "키오스크 화면 크기는 몇 인치로 요구되나요?", "ans": "55인치 가로"},
        {"no": 8, "type": "VECTOR", "q": "키오스크를 조작할 때 마우스나 키보드가 필요한가요?", "ans": "불필요 (터치)"},
        {"no": 9, "type": "VECTOR", "q": "동문검색 소프트웨어에는 몇 기수까지의 명단이 들어가나요?", "ans": "1기~96기"},
        {"no": 10, "type": "VECTOR", "q": "동문 검색 시 어떤 검색 기능이 있어야 하나요?", "ans": "초성 검색"},
        {"no": 11, "type": "VECTOR", "q": "동문 검색 화면에 기본적으로 어떤 정보가 표시되어야 하나요?", "ans": "사진, 이름, 기수, 졸업년도"},
        {"no": 12, "type": "VECTOR", "q": "입찰 신청 시 구비 서류는 어떤 것들이 있나요?", "ans": "제안서, 견적서 등"},
        {"no": 13, "type": "VECTOR", "q": "1차 서류심사에서는 무엇을 평가하나요?", "ans": "제안서류 및 금액"},
        {"no": 14, "type": "VECTOR", "q": "제안 발표회에서 발표와 질의응답 시간은 각각 몇 분인가요?", "ans": "10분 / 20분"},
        {"no": 15, "type": "VECTOR", "q": "키오스크 S/W 자료의 귀속 권한은 누구에게 있나요?", "ans": "기념사업회"},
        {"no": 16, "type": "VECTOR", "q": "구동 PC의 권장 사양은 어떻게 되나요?", "ans": "i5-12400, 6G RAM"},
        {"no": 17, "type": "VECTOR", "q": "재공모 시 기업 단독 출품일 경우 업체 선정은 어떻게 하나요?", "ans": "협상에 의한 선정"},
        {"no": 18, "type": "VECTOR", "q": "보안확약서에 따라 사업수행 중 얻은 자료는 어떻게 처리해야 하나요?", "ans": "반납 및 파기"},
        {"no": 19, "type": "VECTOR", "q": "심사위원회 회의는 투명성을 위해 공개되나요?", "ans": "비공개 원칙"},
        {"no": 20, "type": "VECTOR", "q": "키오스크 설치 시 구조 보강 비용이 발생하면 누가 부담하나요?", "ans": "선정기업"},
        {"no": 21, "type": "VECTOR", "q": "화면 멈춤 현상을 막기 위해 어떤 통신 방식을 사용해야 하나요?", "ans": "RS232C"},
        {"no": 22, "type": "VECTOR", "q": "본 사업의 정확한 계약 주체(기관)는 어디인가요?", "ans": "기념사업회(장학회)"},
        {"no": 23, "type": "VECTOR", "q": "하도급업체가 보안을 위반할 경우 주사업자도 책임을 져야 하나요?", "ans": "동일한 책임"},
        {"no": 24, "type": "VECTOR", "q": "대리인이 참가신청서를 낼 때 추가 서류는 무엇인가요?", "ans": "위임장, 신분증 등"},
        {"no": 25, "type": "VECTOR", "q": "키오스크 구축 사업의 기간은 언제까지인가요?", "ans": "2024. 11. 30"},

       # --- SQL 평가 (정형 데이터) ---
        {"no": 26, "type": "SQL", "q": "기부금 후원 명단에 등록된 사람은 총 몇 명(건)이야?", "ans": "112건"},
        {"no": 27, "type": "SQL", "q": "기부금 후원 내역의 총 모금액(출연금액 합계)은 얼마야?", "ans": "339,000,000원"},
        {"no": 28, "type": "SQL", "q": "49기 동문들이 기부한 건수는 총 몇 건이야?", "ans": "30건"},
        {"no": 29, "type": "SQL", "q": "가장 많은 금액을 한 번에 기부한 사람은 누구고, 얼마야?", "ans": "여*용, 50,000,000원"},
        {"no": 30, "type": "SQL", "q": "'이*곤' 동문이 기부한 총 금액은 얼마야?", "ans": "40,000,000원"},
        {"no": 31, "type": "SQL", "q": "2025년 2월 21일에 기부한 사람은 총 몇 명이야?", "ans": "10명"},
        {"no": 32, "type": "SQL", "q": "55기 이*규 동문은 총 몇 번에 걸쳐 기부했어?", "ans": "3번"},
        {"no": 33, "type": "SQL", "q": "기부 금액이 10,000,000원 이상인 건수는 총 몇 건이야?", "ans": "8건"},
        {"no": 34, "type": "SQL", "q": "자동차과동문회에서 기부한 횟수와 총액은 얼마야?", "ans": "1회, 10,000,000원"},
        {"no": 35, "type": "SQL", "q": "46기 이*삼 동문의 총 기부 금액은 얼마야?", "ans": "1,000,000원"},
        {"no": 36, "type": "SQL", "q": "가장 적은 금액을 기부한 사람들의 이름은 뭐야?", "ans": "전*성, 홍*표, 유*재 (각 100,000원)"},
        {"no": 37, "type": "SQL", "q": "2025년 1월에 기부된 금액의 총합은 얼마야?", "ans": "80,000,000원"},
        {"no": 38, "type": "SQL", "q": "기부 금액이 정확히 200,000원인 건수는 총 몇 건이야?", "ans": "3건"},
        {"no": 39, "type": "SQL", "q": "2025-008 발행번호로 등록된 기부 내역은 총 몇 건이야?", "ans": "3건"},
        {"no": 40, "type": "SQL", "q": "60기 동문 중 가장 큰 금액을 기부한 사람은 누구야?", "ans": "안*태, 박*윤 (각 1,000,000원)"},
        {"no": 41, "type": "SQL", "q": "2025년 3월 접수된 건 중 가장 기부금이 큰 건은 얼마야?", "ans": "50,000,000원 (여*용)"},
        {"no": 42, "type": "SQL", "q": "송**랑 동문이 기부한 금액과 기수는 어떻게 돼?", "ans": "1,000,000원, 56기"},
        {"no": 43, "type": "SQL", "q": "56기 동문들이 기부한 횟수는 총 몇 번이야?", "ans": "20번"},
        {"no": 44, "type": "SQL", "q": "49기 동문들의 기부금 총액은 얼마야?", "ans": "24,000,000원"},
        {"no": 45, "type": "SQL", "q": "기부 금액 상위 3명의 이름과 금액을 알려줘.", "ans": "여*용(5천만), 이*곤(3.7천만), 노*찬(2.5천만)"},
        {"no": 46, "type": "SQL", "q": "(주)*산 회사에서 기부한 금액은 얼마야?", "ans": "1,000,000원"},
        {"no": 47, "type": "SQL", "q": "발행번호가 2025-100 이후인 건수는 몇 건이야?", "ans": "13건"},
        {"no": 48, "type": "SQL", "q": "58기 중 이름이 '김'으로 시작하는 사람은 누구야?", "ans": "김*호, 김*수"},
        {"no": 49, "type": "SQL", "q": "출연일자가 2025년 5월인 사람들의 이름과 금액은?", "ans": "정*호(1백만), 박*기(1백만) 등 총 13명"},
        {"no": 50, "type": "SQL", "q": "1,000,000원을 기부한 내역은 총 몇 건이야?", "ans": "65건"}
    ]

def run_experiment():
    print("\n" + "="*50)
    print(" 🚀 [논문 실험] 하이브리드 VECTOR 50문항 자동 평가 시작")
    print("="*50 + "\n")

    results = []
    headers = {"X-API-Key": API_KEY}
    
    total_count = len(eval_data)
    route_correct_count = 0

    for item in eval_data:
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
                ans_check = "O" if str(item['ans']).split(',')[0].replace("원","").replace("건","").strip() in ai_ans else "△"
                
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

if __name__ == "__main__":
    run_experiment()