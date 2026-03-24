import os
import chromadb
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

# 1. PostgreSQL 연결 설정 (정형 데이터용)
POSTGRES_USER = "admin"
POSTGRES_PASSWORD = "1q2w3e4r"
POSTGRES_DB = "rag_database"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"

SQLALCHEMY_DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_postgres_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# 2. ChromaDB 연결 설정 (비정형 데이터용)
# 도커로 띄운 ChromaDB(포트 8000)에 연결합니다.
chroma_client = chromadb.HttpClient(host='localhost', port=8000)

def get_chroma_collection(collection_name="scholarship_rules"):
    # 규정집 텍스트를 담을 컬렉션을 가져오거나 새로 만듭니다.
    return chroma_client.get_or_create_collection(name=collection_name)