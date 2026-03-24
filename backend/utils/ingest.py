import os
import sys
import re
import subprocess
import shutil

# 현재 파일의 상위 폴더(backend)를 경로에 추가 (database.py 인식용)
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import pandas as pd
import pdfplumber
from bs4 import BeautifulSoup
from database import engine, get_chroma_collection

# ---------------------------------------------------------
# 1. 정형 데이터 (xlsx) -> PostgreSQL
# ---------------------------------------------------------
def ingest_xlsx_to_postgres(file_path):
    print(f"[엑셀 처리 중] {file_path}")
    try:
        df = pd.read_excel(file_path)
        file_name = os.path.basename(file_path).split('.')[0]
        
        # 테이블명 정제 (영문, 숫자, 언더바만 허용)
        table_name = re.sub(r'[^a-zA-Z0-9_]', '_', file_name)
        if table_name[0].isdigit():
            table_name = "tbl_" + table_name
            
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"  -> PostgreSQL '{table_name}' 테이블 적재 완료!")
    except Exception as e:
        print(f"  -> 엑셀 처리 에러: {e}")

# ---------------------------------------------------------
# 2. 하이브리드 데이터 (pdf) -> 표는 PostgreSQL, 글은 ChromaDB
# ---------------------------------------------------------
def ingest_pdf_hybrid(file_path):
    print(f"[PDF 하이브리드 처리 중] {file_path}")
    full_text = []
    table_count = 0
    file_name = os.path.basename(file_path).split('.')[0]
    safe_file_name = re.sub(r'[^a-zA-Z0-9_]', '_', file_name)
    if safe_file_name[0].isdigit():
        safe_file_name = "tbl_" + safe_file_name
    
    try:
        with pdfplumber.open(file_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                # 1. 텍스트 추출 (ChromaDB용)
                text_data = page.extract_text()
                if text_data:
                    full_text.append(text_data)
                
                # 2. 표(Table) 추출 (PostgreSQL용)
                tables = page.extract_tables()
                for table in tables:
                    if not table or len(table) < 2:
                        continue
                    try:
                        df = pd.DataFrame(table[1:], columns=table[0])
                        df = df.dropna(how='all')
                        df = df.replace('\n', ' ', regex=True)
                        
                        db_table_name = f"{safe_file_name}_p{page_num}_t{table_count}"
                        df.to_sql(db_table_name, engine, if_exists='replace', index=False)
                        table_count += 1
                    except Exception as e:
                        pass # 표 변환 에러 시 건너뜀

        # 추출된 텍스트가 있다면 ChromaDB로 전송
        if full_text:
            text_chunks = [p.strip() for text_page in full_text for p in text_page.split('\n') if len(p.strip()) > 5]
            if text_chunks:
                save_to_chroma(file_path, text_chunks)
            
        if table_count > 0:
            print(f"  -> 추출된 {table_count}개의 표를 PostgreSQL에 적재 완료!")
        elif not full_text:
            print("  -> 추출된 데이터가 없습니다. (스캔본 이미지일 수 있습니다)")
            
    except Exception as e:
        print(f"  -> PDF 처리 에러: {e}")

# ---------------------------------------------------------
# 3. 비정형 데이터 (hwp) -> HTML 변환 후 표/텍스트 완벽 분리 (BS4 수동 추출)
# ---------------------------------------------------------
def convert_hwp_to_html_and_ingest(file_path):
    print(f"[HWP -> HTML 자동 변환 및 처리 중] {file_path}")
    
    file_name = os.path.basename(file_path).split('.')[0]
    safe_file_name = re.sub(r'[^a-zA-Z0-9_]', '_', file_name)
    if safe_file_name[0].isdigit():
        safe_file_name = "tbl_" + safe_file_name
        
    # HTML이 저장될 임시 폴더
    html_output_dir = os.path.join(os.path.dirname(file_path), f"temp_{safe_file_name}")
    
    try:
        # 1. HWP -> HTML 변환
        subprocess.run(
            ['hwp5html', '--output', html_output_dir, file_path],
            capture_output=True, text=True, shell=True
        )
        
        index_html_path = os.path.join(html_output_dir, "index.xhtml")
        
        if not os.path.exists(index_html_path):
            print("  -> HTML 변환 실패 (지원되지 않는 HWP 버전이거나 암호화 문서입니다)")
            return
            
        # 2. 변환된 HTML 파일 읽기
        with open(index_html_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
            
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # --- [A] 표(Table) 수동 추출하여 PostgreSQL에 넣기 ---
        table_count = 0
        tables = soup.find_all('table')
        
        for i, table in enumerate(tables):
            rows = table.find_all('tr')
            table_data = []
            
            for row in rows:
                # 각 칸(td, th)을 찾아서 안에 있는 지저분한 태그를 무시하고 텍스트만 뽑음
                cols = row.find_all(['td', 'th'])
                col_text = [col.get_text(strip=True) for col in cols]
                
                # 빈 행이 아니면 데이터에 추가
                if any(col_text):
                    table_data.append(col_text)
            
            # 표가 2줄 이상(제목+내용)일 때만 DB에 넣기
            if len(table_data) >= 2:
                try:
                    # 병합된 셀(colspan) 때문에 열 개수가 안 맞을 수 있으므로 빈칸으로 채워줌
                    max_cols = max(len(r) for r in table_data)
                    normalized_data = [r + [''] * (max_cols - len(r)) for r in table_data]
                    
                    # 첫 번째 줄을 컬럼명(헤더)으로 지정
                    df = pd.DataFrame(normalized_data[1:], columns=normalized_data[0])
                    
                    df.columns = [
                    str(c).strip() if str(c).strip() not in ['', 'None', 'nan'] else f"unnamed_{j}" 
                    for j, c in enumerate(df.columns)
                    ]
                    
                    db_table_name = f"{safe_file_name}_html_t{i}"
                    df.to_sql(db_table_name, engine, if_exists='replace', index=False)
                    table_count += 1
                except Exception as e:
                    print(f"  -> {i}번째 표 파싱 중 에러 (건너뜀): {e}")
                    
        if table_count > 0:
            print(f"  -> 추출된 {table_count}개의 HWP 표를 PostgreSQL에 적재 완료!")

        # --- [B] 순수 텍스트(Text) 추출하여 ChromaDB에 넣기 ---
        # 표 부분은 이미 DB에 넣었으므로 HTML에서 표 태그를 통째로 삭제
        for table in soup.find_all('table'):
            table.decompose()
            
        text_data = soup.get_text(separator='\n')
        paragraphs = [p.strip() for p in text_data.split('\n') if len(p.strip()) > 5]
        
        if paragraphs:
            save_to_chroma(file_path, paragraphs)
            
    except Exception as e:
        print(f"  -> HWP 파싱 에러: {e}")
        
    finally:
        # 임시 생성된 폴더 삭제
        if os.path.exists(html_output_dir):
            shutil.rmtree(html_output_dir, ignore_errors=True)

# ---------------------------------------------------------
# 공통 저장 로직 (ChromaDB)
# ---------------------------------------------------------
def save_to_chroma(file_path, text_chunks):
    collection = get_chroma_collection("scholarship_rules")
    doc_name = os.path.basename(file_path)
    ids = [f"{doc_name}_chunk_{i}" for i in range(len(text_chunks))]
    metadatas = [{"source": doc_name} for _ in range(len(text_chunks))]
    
    collection.add(
        documents=text_chunks,
        metadatas=metadatas,
        ids=ids
    )
    print(f"  -> ChromaDB '{doc_name}' ({len(text_chunks)}조각) 텍스트 임베딩 완료!")

# ---------------------------------------------------------
# 메인 실행부
# ---------------------------------------------------------
def process_file(file_path):
    ext = file_path.split('.')[-1].lower()
    
    if ext == 'xlsx':
        ingest_xlsx_to_postgres(file_path)
    elif ext == 'pdf':
        ingest_pdf_hybrid(file_path)
    elif ext == 'hwp':
        convert_hwp_to_html_and_ingest(file_path)
    else:
        print(f"지원하지 않는 확장자입니다: {ext}")

if __name__ == "__main__":
    # 데이터 폴더 경로 지정
    data_folder = os.path.join(os.path.dirname(__file__), "..", "data")
    
    if not os.path.exists(data_folder):
        print(f"'{data_folder}' 폴더를 찾을 수 없습니다. backend 폴더 안에 data 폴더를 만들어주세요.")
    else:
        files = os.listdir(data_folder)
        for f in files:
            if not f.startswith('.') and f.lower().endswith(('xlsx', 'pdf', 'hwp')):
                file_path = os.path.join(data_folder, f)
                process_file(file_path)
                
        print("\n모든 파일의 적재가 완료되었습니다!")