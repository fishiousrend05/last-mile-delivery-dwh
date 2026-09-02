"""
utils/database.py — Quản lý kết nối Postgres dùng chung cho toàn bộ ingestion
pipeline (loaders ghi landing table, metadata.py ghi audit log, validators
đọc lại để check nếu cần).
 
Dùng 1 SQLAlchemy Engine duy nhất / process (cached qua lru_cache) thay vì
mỗi module tự tạo engine riêng — tránh mở quá nhiều connection khi Prefect
chạy song song nhiều task trong cùng 1 flow run.
"""
 
import os
from contextlib import contextmanager
from functools import lru_cache
 
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
 
load_dotenv()
 
 
def _build_connection_url() -> str:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD", "")
 
    missing = [name for name, val in [("POSTGRES_DB", db), ("POSTGRES_USER", user)] if not val]
    if missing:
        raise EnvironmentError(
            f"Missing required env vars: {', '.join(missing)}. "
            "Check your .env file against .env.example."
        )
 
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
 
 
@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """
    SQLAlchemy Engine dùng chung, chỉ tạo 1 lần / process nhờ lru_cache.
    pool_pre_ping=True để tự phát hiện connection chết (vd Postgres restart
    giữa lúc flow đang chạy dài) thay vì raise lỗi khó hiểu ở query bất kỳ.
    """
    return create_engine(
        _build_connection_url(),
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        future=True,
    )
 
 
_SessionFactory = sessionmaker(bind=None, future=True)
 
 
@contextmanager
def get_session():
    """
    Context manager cấp Session cho các thao tác cần transaction rõ ràng
    (chủ yếu dùng trong metadata.py để ghi audit log). Tự commit khi thành
    công, tự rollback khi có exception, luôn close session sau khi dùng.
 
    Usage:
        with get_session() as session:
            session.execute(...)
    """
    _SessionFactory.configure(bind=get_engine())
    session: Session = _SessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
 
 
def test_connection() -> bool:
    """
    Kiểm tra kết nối Postgres còn sống — gọi ở đầu mỗi Prefect flow run để
    fail fast, thay vì để lỗi kết nối xảy ra giữa chừng sau khi đã extract
    xong 1 bảng lớn (tốn thời gian gọi API vô ích).
    """
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False