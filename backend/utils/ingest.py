'''
pdf 파일과 같이 표와 텍스트가 섞여 있는 경우,
표는 구조화 데이터로 PostgreSQL에 저장하고, 
텍스트는 ChromaDB에 저장함.
'''

import os
import sys
import re
import subprocess
import shutil
import hashlib
import logging
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
import pandas as pd
import pdfplumber
from bs4 import BeautifulSoup
from sqlalchemy import text, inspect

# 현재 파일의 상위 폴더(backend)를 경로에 추가 (database.py 인식용)
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from database import engine, get_chroma_collection

#  로깅 설정 함수
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "..", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "ingest.log")

logger = logging.getLogger("ingest")
logger.setLevel(logging.INFO)

if not logger.handlers:
    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s %(name)s - %(message)s"
    )

    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

# 공통적으로 사용되는 보조 함수들 추가
def sanitize_table_name(file_name: str) -> str:
    """
    - 파일명을 PostgreSQL 테이블명으로 안전하게 변환.
    - 영문/숫자/언더바(_) 외 문자는 _로 치환
    - 비어 있으면 기본 이름(tbl_unnamed) 부여
    - 숫자로 시작하면 'tbl_' 접두어 추가
    """
    table_name = re.sub(r"[^a-zA-Z0-9_]", "_", file_name)
    if not table_name:
        table_name = "tbl_unnamed"
    if table_name[0].isdigit():
        table_name = "tbl_" + table_name
    return table_name


def compute_file_md5(file_path: str, chunk_size: int = 8192) -> str:
    """
    - 파일 내용 기준 MD5 해시를 계산해서 이전에 처리했던 파일과 내용이 같은지 비교.
    - 파일 전체를 한 번에 읽지 않고 chunk 단위로 읽어 메모리 부담 저하.,
    """
    md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        while chunk := f.read(chunk_size):
            md5.update(chunk)
    return md5.hexdigest()


def infer_category(file_path: str) -> str:
    """
    - 파일이 위치한 상위 폴더명을 카테고리로 분류.
    - 문서 분류에 도움이 될 수 있다(상위 폴더가 기본 폴더(data) 일 시 uncategorizedf로 분류).
    """
    parent = os.path.basename(os.path.dirname(file_path))
    if parent.lower() == "data":
        return "uncategorized"
    return parent


def get_uploaded_at(file_path: str) -> str:
    """
    - 파일의 수정 시각을 ISO 형식 문자열로 반환하여 메타데이터로 사용.
    - 해당 메타데이터는 ChromaDB 메타데이터에 저장.
    """
    ts = os.path.getmtime(file_path)
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def split_text_chunks(text: str, min_len: int = 5):
    '''
    - 긴 텍스트를 줄 단위의 청크로 분리
    '''
    return [line.strip() for line in text.splitlines() if len(line.strip()) > min_len]

# utf-8만으로 인코딩 하는 것이 아니라 유사 시 cp949, euc-kr 으로도 인코팅 시도.
def read_text_with_fallbacks(file_path: str, encodings=None) -> str:
    if encodings is None:
        encodings = ["utf-8", "cp949", "euc-kr"]

    with open(file_path, "rb") as f:
        raw = f.read()

    for enc in encodings:
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue

    return raw.decode("utf-8", errors="replace")

# 중복 방지용 manifest 테이블 r관련 기능
def ensure_manifest_table():
    """
    - 적재 이력을 저장할 ingestion_manifest 테이블(관리용)을 생성.
    - 어떤 파일을 처리했는지 기록.
    - 마지막 처리 당시의 해시값 저장.
    - 성공/실패 상태 저장.
    - 오류 메시지 저장.
    - ChromaDB에 몇 개의 청크를 넣었는지 기록
    """
    query = """
    CREATE TABLE IF NOT EXISTS ingestion_manifest (
        source TEXT PRIMARY KEY,
        source_path TEXT,
        file_hash TEXT NOT NULL,
        file_type TEXT,
        category TEXT,
        processed_at TIMESTAMP NOT NULL,
        status TEXT NOT NULL,
        error_message TEXT,
        chroma_doc_count INTEGER DEFAULT 0
    );
    """
    with engine.begin() as conn:
        conn.execute(text(query))


def get_existing_file_hash(source: str):
    """
    - 파일명으로 이전에 처리한 해시값을 조회.
    - 현재 파일의 해시와 비교해서 내용이 바뀌지 않았으면 적재를 생략.
    """
    query = text("""
        SELECT file_hash
        FROM ingestion_manifest
        WHERE source = :source
    """)
    with engine.begin() as conn:
        row = conn.execute(query, {"source": source}).fetchone()
        return row[0] if row else None


def upsert_manifest(
    source: str,
    source_path: str,
    file_hash: str,
    file_type: str,
    category: str,
    status: str,
    error_message: str = None,
    chroma_doc_count: int = 0,
):
    """
    - ingestion_manifest 테이블에 파일 처리 결과를 저장하거나 갱신.
    - 파일 경로, 해시, 확장자, 카테고리, 처리 시각, 성공/실패 여부, 오류 메시지, Chroma 적재 건수를 저장.
    """
    query = text("""
        INSERT INTO ingestion_manifest (
            source, source_path, file_hash, file_type, category,
            processed_at, status, error_message, chroma_doc_count
        )
        VALUES (
            :source, :source_path, :file_hash, :file_type, :category,
            :processed_at, :status, :error_message, :chroma_doc_count
        )
        ON CONFLICT (source)
        DO UPDATE SET
            source_path = EXCLUDED.source_path,
            file_hash = EXCLUDED.file_hash,
            file_type = EXCLUDED.file_type,
            category = EXCLUDED.category,
            processed_at = EXCLUDED.processed_at,
            status = EXCLUDED.status,
            error_message = EXCLUDED.error_message,
            chroma_doc_count = EXCLUDED.chroma_doc_count
    """)
    with engine.begin() as conn:
        conn.execute(query, {
            "source": source,
            "source_path": source_path,
            "file_hash": file_hash,
            "file_type": file_type,
            "category": category,
            "processed_at": datetime.now(),
            "status": status,
            "error_message": error_message,
            "chroma_doc_count": chroma_doc_count,
        })

# 자료 중복시 기존 테이블 갱신 후 삭제를 위한 함수.
def drop_tables_with_prefix(prefix: str):
    """
    - 특정 prefix로 시작하는 기존 PostgreSQL 테이블들을 삭제.
    - 중복을 방지하기 위해서 동일 문서의 재적재 전에 기존 파생 테이블을 정리.
    """
    inspector = inspect(engine)
    table_names = inspector.get_table_names(schema="public")
    targets = [t for t in table_names if t.startswith(prefix)]

    if not targets:
        return

    with engine.begin() as conn:
        for table_name in targets:
            conn.execute(text(f'DROP TABLE IF EXISTS public."{table_name}" CASCADE'))

    # 자료 중복시 기존 테이블 갱신 후 삭제
    logger.info("기존 테이블 %d개 삭제 완료 (prefix=%s)", len(targets), prefix)

# ---------------------------------------------------------
# 1. 정형 데이터 (xlsx) -> PostgreSQL
# ---------------------------------------------------------
'''
def ingest_xlsx_to_postgres(file_path):
    print(f"[엑셀 처리 중] {file_path}")
    try:
        df = pd.read_excel(file_path, engine="openpyxl")
        file_name = os.path.basename(file_path).split('.')[0]
        
        # 테이블명 정제 (영문, 숫자, 언더바만 허용)
        table_name = re.sub(r'[^a-zA-Z0-9_]', '_', file_name)
        if table_name[0].isdigit():
            table_name = "tbl_" + table_name
            
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"  -> PostgreSQL '{table_name}' 테이블 적재 완료!")
    except Exception as e:
        print(f"  -> 엑셀 처리 에러: {e}")
'''

def ingest_xlsx_to_postgres(file_path):
    # 처리하는 파일의 경로를 추가로 표시.
    # Excel 파일을 PostgreSQL 테이블로 적재한다.
    logger.info("[엑셀 처리 중] %s", file_path)
    try:
        df = pd.read_excel(file_path, engine="openpyxl")
        file_name = os.path.basename(file_path).split('.')[0]

        table_name = sanitize_table_name(file_name)

        df.to_sql(table_name, engine, if_exists='replace', index=False)
        # 적재를 한 DB의 종류와(여기서는 postgreSQL), 테이블 이름까지 로깅.
        logger.info("PostgreSQL '%s' 테이블 적재 완료", table_name)
    except Exception:
        # 처리하는 파일의 이름을 로그에 남겨 오류 발생 시 원인 탐색을 용이하게 함.
        logger.exception("엑셀 처리 에러 | file=%s", file_path)
        raise

# ---------------------------------------------------------
# 2. 하이브리드 데이터 (pdf) -> 표는 PostgreSQL, 글은 ChromaDB
# ---------------------------------------------------------
'''
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
'''

def ingest_pdf_hybrid(file_path: str, file_hash: str, category: str) -> int:
    logger.info("[PDF 하이브리드 처리 중] %s", file_path)
    '''
    - pdf를 적재하는 함수.
    - ChromaDB에 저장된 텍스트 청크 수 반환.
    '''

    file_name = os.path.basename(file_path).split(".")[0]
    safe_file_name = sanitize_table_name(file_name)

    # 같은 pdf파일을 재적재할때 이전의 테이블이 남지 않도록 해당 문서의 PostgreSQL 테이블을 제거.
    drop_tables_with_prefix(f"{safe_file_name}_p")

    # pdf 텍스트에서 추출한 청크 수와 페이지 번호를 담아 메타데이터로 사용.
    chunk_records = []
    table_count = 0

    with pdfplumber.open(file_path) as pdf:
        # 줄 단위로 ChromaDB 검색용 청크 분리.
        # 페이지 번호를 함께 저장(메타데이터로)해 출처 추적에 사용
        for page_num, page in enumerate(pdf.pages, start=1):
            try:
                text_data = page.extract_text()
                if text_data:
                    chunks = split_text_chunks(text_data)
                    for chunk in chunks:
                        chunk_records.append({
                            "text": chunk,
                            "page": page_num,
                        })
            except Exception:
                logger.exception("PDF 텍스트 추출 실패 | file=%s page=%d", file_path, page_num)

            try:
                # 페이지 내부 표(테이블) 추출
                tables = page.extract_tables()
            except Exception:
                logger.exception("PDF 표 목록 추출 실패 | file=%s page=%d", file_path, page_num)
                continue

            for table_idx, table in enumerate(tables):
                if not table or len(table) < 2:
                    continue

                try:
                    # 첫 행을 컬럼명으로 사용, 빈 행 제거, 개행 정리 후 SQL테이블로 저장.
                    df = pd.DataFrame(table[1:], columns=table[0])
                    df = df.dropna(how="all")
                    df = df.replace("\n", " ", regex=True)

                    db_table_name = f"{safe_file_name}_p{page_num}_t{table_count}"
                    df.to_sql(db_table_name, engine, if_exists="replace", index=False)
                    table_count += 1

                except Exception:
                    logger.exception(
                        "PDF 표 저장 실패 | file=%s page=%d table_index=%d",
                        file_path,
                        page_num,
                        table_idx,
                    )

    chroma_count = 0
    if chunk_records:
        chroma_count = save_to_chroma(file_path, chunk_records, file_hash, category)
    # 표, 텍스트 없는 경우에는 스캔본 이미지일 수 있음.
    elif table_count == 0:
        logger.warning("추출된 데이터가 없습니다. 스캔본 이미지일 수 있습니다. | file=%s", file_path)

    logger.info("PDF 처리 완료 | file=%s tables=%d chroma_chunks=%d", file_path, table_count, chroma_count)
    return chroma_count

# ---------------------------------------------------------
# 3. 비정형 데이터 (hwp) -> HTML 변환 후 표/텍스트 완벽 분리 (BS4 수동 추출)
# ---------------------------------------------------------
'''
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
'''

def convert_hwp_to_html_and_ingest(file_path: str, file_hash: str, category: str) -> int:
    """
    - HWP 파일을 HTML로 변환한 뒤, 표와 본문을 분리해 저장.
    - hwp5html로 HWP를 HTML로 변환.
    - HTML에서 표는 PostgreSQL로 저장.
    - 표를 제거한 본문 텍스트는 ChromaDB로 저장.

    - ChromaDB에 저장된 텍스트 청크의0 수 반환.
    """
    
    logger.info("[HWP -> HTML 자동 변환 및 처리 중] %s", file_path)

    file_name = os.path.basename(file_path).split(".")[0]
    safe_file_name = sanitize_table_name(file_name)

    # 같은 파일을 다시 적재할 경우, 이전에 생성된 테이블이 남지 않도록 기존 파생 테이블을 먼저 삭제.
    drop_tables_with_prefix(f"{safe_file_name}_html_t")

    # HWP를 바로 파싱하지 않고 HTML로 변환한 결과를 임시 폴더에 저장.
    html_output_dir = os.path.join(os.path.dirname(file_path), f"temp_{safe_file_name}")

    try:
        # 변환 실패 시 즉시 종료.
        result = subprocess.run(
            ["hwp5html", "--output", html_output_dir, file_path],
            capture_output=True,
            text=True,
            shell=False,
            check=False,
        )

        if result.returncode != 0:
            logger.error(
                "HWP -> HTML 변환 실패 | file=%s returncode=%s stderr=%s",
                file_path,
                result.returncode,
                result.stderr.strip() if result.stderr else "",
            )
            return 0

        index_html_path = os.path.join(html_output_dir, "index.xhtml")
        if not os.path.exists(index_html_path):
            logger.error("HTML 변환 실패 (index.xhtml 없음) | file=%s", file_path)
            return 0

        html_content = read_text_with_fallbacks(index_html_path)
        soup = BeautifulSoup(html_content, "html.parser")

        table_count = 0
        tables = soup.find_all("table")

        for i, table in enumerate(tables):
            rows = table.find_all("tr")
            table_data = []

            for row in rows:
                cols = row.find_all(["td", "th"])
                col_text = [col.get_text(strip=True) for col in cols]
                if any(col_text):
                    table_data.append(col_text)

            if len(table_data) < 2:
                continue

            try:
                # 병합 셀(colspan) 등으로 행마다 열 수가 다를 수 있기 때문에, 가장 긴 행 기준으로 빈칸을 채워 데이터프레임 형태를 맞춤.
                max_cols = max(len(r) for r in table_data)
                normalized_data = [r + [""] * (max_cols - len(r)) for r in table_data]

                df = pd.DataFrame(normalized_data[1:], columns=normalized_data[0])

                # 비어 있는 컬럼명은 unnamed_* 형태로 보정.
                df.columns = [
                    str(c).strip() if str(c).strip() not in ["", "None", "nan"] else f"unnamed_{j}"
                    for j, c in enumerate(df.columns)
                ]

                db_table_name = f"{safe_file_name}_html_t{i}"
                df.to_sql(db_table_name, engine, if_exists="replace", index=False)
                table_count += 1

            except Exception:
                logger.exception("HWP 표 저장 실패 | file=%s table_index=%d", file_path, i)

        if table_count > 0:
            logger.info("추출된 %d개의 HWP 표를 PostgreSQL에 적재 완료 | file=%s", table_count, file_path)

        # 표는 이미 PostgreSQL에 저장했기 때문에, 본문 텍스트만 남기기 위해 table 태그를 제거.
        for table in soup.find_all("table"):
            table.decompose()

        text_data = soup.get_text(separator="\n")
        paragraphs = split_text_chunks(text_data)

        chunk_records = [{"text": p, "page": None} for p in paragraphs]
        chroma_count = 0
        if chunk_records:
            chroma_count = save_to_chroma(file_path, chunk_records, file_hash, category)

        logger.info("HWP 처리 완료 | file=%s tables=%d chroma_chunks=%d", file_path, table_count, chroma_count)
        return chroma_count

    # 변환 과정에서 생성한 임시 폴더는 작업 후 무조건 정리됨(찌꺼기 파일 방지).
    finally:
        if os.path.exists(html_output_dir):
            shutil.rmtree(html_output_dir, ignore_errors=True)

# ---------------------------------------------------------
# 공통 저장 로직 (ChromaDB)
# ---------------------------------------------------------
'''
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
'''
# 메타데이터 확장을 위해  \save_to_chroma 함수 교체
def save_to_chroma(file_path: str, chunk_records, file_hash: str, category: str) -> int:
    """
    - 텍스트 청크 ChromaDB에 저장하는 함수.
    - 같은 source 문서가 이미 있으면 먼저 삭제.
    - 현재 파일 기준으로 metadata를 구성.
    - upsert 방식으로 문서와 metadata를 저장.

    - 메타데이터에 저장되는 정보
    - source, source_path
    - file_type, category
    - file_hash
    - chunk_index
    - uploaded_at, ingested_at
    - page (페이지가 존재하는 문서의 경우)
    """
    
    collection = get_chroma_collection("scholarship_rules")
    doc_name = os.path.basename(file_path)
    abs_path = os.path.abspath(file_path)
    ext = os.path.splitext(file_path)[1].lower().lstrip(".")
    uploaded_at = get_uploaded_at(file_path)
    ingested_at = datetime.now(timezone.utc).isoformat()

    try:
        collection.delete(where={"source": doc_name})
        logger.info("기존 Chroma 문서 삭제 완료: %s", doc_name)
    except Exception:
        logger.info("기존 Chroma 문서 없음 또는 삭제 스킵: %s", doc_name)

    documents = []
    metadatas = []
    ids = []

    for idx, item in enumerate(chunk_records):
        text_value = item["text"].strip()
        if not text_value:
            continue

        page = item.get("page")

        # 검색 결과의 출처 추적과 필터링을 위해 파일 정보와 청크 단위 정보를 메타데이터에 저장.
        metadata = {
            "source": doc_name,
            "source_path": abs_path,
            "file_type": ext,
            "category": category,
            "file_hash": file_hash,
            "chunk_index": idx,
            "uploaded_at": uploaded_at,
            "ingested_at": ingested_at,
        }
        # 페이지 정보가 있는 문서는 페이지를 함께 저장해 몇 페이지에서 나온 내용인지 추적 가능하게 함.
        if page is not None:
            metadata["page"] = page

        documents.append(text_value)
        metadatas.append(metadata)
        ids.append(f"{doc_name}::chunk::{idx}")

    if not documents:
        logger.info("Chroma 저장 대상 텍스트 없음: %s", doc_name)
        return 0

    collection.upsert(
        documents=documents,
        metadatas=metadatas,
        ids=ids,
    )

    logger.info("ChromaDB '%s' 텍스트 임베딩 완료 (%d chunks)", doc_name, len(documents))
    return len(documents)

# ---------------------------------------------------------
# 메인 실행부
# ---------------------------------------------------------
'''
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
'''

# process_file 함수 교체 - 로깅, 중복 방지, manifest 기록 등 추가
def process_file(file_path: str):
    """
    단일 파일을 처리하는 메인 진입점.

    1) 파일 경로/확장자/카테고리/해시 계산.
    2) manifest에서 기존 해시와 비교해 변경 여부 확인.
    3) 파일 형식별 처리 함수 호출.
    4) 성공/실패 결과를 manifest에 기록.

    매개변수 : file_path: 처리할 원본 파일의 경로
    """
    source = os.path.basename(file_path)
    source_path = os.path.abspath(file_path)
    ext = os.path.splitext(file_path)[1].lower().lstrip(".")
    category = infer_category(file_path)
    file_hash = compute_file_md5(file_path)

    # 이전 처리 이력과 현재 파일 해시를 비교하여 내용이 바뀌지 않았다면 재적재하지 않고 바로 종료.
    existing_hash = get_existing_file_hash(source)
    if existing_hash == file_hash:
        logger.info("생략 : 변경 없음 | file=%s hash=%s", file_path, file_hash)
        return

    logger.info("시작 : file=%s type=%s category=%s hash=%s", file_path, ext, category, file_hash)

    try:
        chroma_doc_count = 0

        if ext == "xlsx":
            ingest_xlsx_to_postgres(file_path)

        elif ext == "pdf":
            chroma_doc_count = ingest_pdf_hybrid(file_path, file_hash, category)

        elif ext == "hwp":
            chroma_doc_count = convert_hwp_to_html_and_ingest(file_path, file_hash, category)

        else:
            logger.warning("지원하지 않는 확장자입니다: %s", ext)
            return
        
        # 정상 처리된 경우 manifest에 성공 상태와 처리 결과를 저장.
        upsert_manifest(
            source=source,
            source_path=source_path,
            file_hash=file_hash,
            file_type=ext,
            category=category,
            status="SUCCESS",
            error_message=None,
            chroma_doc_count=chroma_doc_count,
        )

        logger.info("[DONE] file=%s", file_path)

    except Exception as e:
        # 예외가 발생해도 실패 이력은 manifest에 저장.
        upsert_manifest(
            source=source,
            source_path=source_path,
            file_hash=file_hash,
            file_type=ext,
            category=category,
            status="FAILED",
            error_message=str(e),
            chroma_doc_count=0,
        )
        logger.exception("[FAILED] file=%s", file_path)

if __name__ == "__main__":
    # 스크립트를 직접 실행했을 때의 시작 지점.
    # 먼저 manifest 테이블을 준비한 뒤, data 폴더의 지원 확장자 파일들을 순회하며 하나씩 처리.
    # manifest 테이블이 없으면 중복 검사/처리 결과 기록이 불가능하므로 파일 처리 전에 반드시 먼저 생성(ensure_manifest_table()호출로).
    ensure_manifest_table()
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