#Chịu trách nhiệm ghi lại "nhật ký vận hành" (audit logs). Ví dụ: Job A chạy lúc mấy giờ, kéo được bao nhiêu dòng, có bao nhiêu dòng lỗi.
"""
utils/metadata.py — Audit log cho MỖI lần Extract-Load chạy (mỗi Prefect
task), bất kể load_mode là full_refresh hay append.
 
QUAN TRỌNG — phân biệt 2 khái niệm "incremental" trong project này, vì đây
là chỗ dễ nhầm nhất:
 
1. dbt incremental materialization trên fact_delivery_attempts (đã chốt ở
   Phase thiết kế) — xử lý HOÀN TOÀN trong dbt, bằng macro is_incremental()
   so sánh với watermark column ngay trên chính fact table đó trong
   Postgres. KHÔNG liên quan gì đến file này.
 
2. Ingestion audit trail (chính là file này) — ghi lại mỗi lần Prefect flow
   chạy extract+load 1 bảng: chạy lúc nào, bao nhiêu dòng, thành công hay
   lỗi. Mục đích chính là debug/monitoring, KHÔNG phải để chạy incremental
   load ở tầng ingestion — vì mọi nguồn của project này (Olist CSV tĩnh,
   weather/holiday theo date range cố định 2016-2018, synthetic seed=42 cố
   định) đều là full load 1 lần, không có khái niệm "chỉ lấy dữ liệu mới"
   ở tầng Extract.
 
   Cột watermark_value vẫn được để sẵn trong schema, nhưng hiện tại sẽ luôn
   NULL — chỉ hữu ích nếu sau này project mở rộng sang nguồn dữ liệu sống
   (vd Olist API thật thay vì CSV tĩnh). KHÔNG cần logic dùng watermark này
   ngay bây giờ.
 
=> Kết luận: bảng audit log này CẦN dựng ngay từ Phase 1, vì loaders sẽ gọi
   nó ở MỌI lần load để ghi nhận run — không phải tính năng "để dành tới
   khi cần incremental" như câu hỏi ban đầu.
"""
 
import uuid
from datetime import datetime, timezone
 
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, Text, text
 
from ingestion.utils.database import get_engine, get_session
from ingestion.utils.logger import get_logger
 
logger = get_logger(__name__)
 
_metadata = MetaData()
 
# Đặt ở schema "audit" riêng, tách khỏi schema "landing" chứa dữ liệu
# nghiệp vụ thật (olist/weather/holidays/synthetic) — đây là bảng vận hành
# của pipeline, không phải bảng nghiệp vụ.
ingestion_audit_log = Table(
    "ingestion_audit_log",
    _metadata,
    Column("run_id", String(36), primary_key=True),
    Column("source_name", String(50), nullable=False),   # olist / weather / holidays / synthetic
    Column("table_name", String(100), nullable=False),
    Column("load_mode", String(20), nullable=False),      # full_refresh / append
    Column("status", String(20), nullable=False),         # running / success / failed
    Column("row_count", Integer, nullable=True),
    Column("error_message", Text, nullable=True),
    Column("watermark_value", String(50), nullable=True),  # để dành cho tương lai, hiện luôn NULL
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True), nullable=True),
    schema="audit",
)
 
 
def init_metadata_table() -> None:
    """
    Tạo schema `audit` + bảng ingestion_audit_log nếu chưa tồn tại.
    CREATE ... IF NOT EXISTS nên idempotent — gọi lại nhiều lần (vd mỗi lần
    flow start) không gây lỗi, không cần điều kiện "chỉ chạy 1 lần" bên ngoài.
    """
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS audit"))
    _metadata.create_all(engine, tables=[ingestion_audit_log])
    logger.info("Metadata table 'audit.ingestion_audit_log' ready")
 
 
def start_run(source_name: str, table_name: str, load_mode: str) -> str:
    """
    Ghi nhận bắt đầu 1 lần extract+load. Trả về run_id — loader PHẢI giữ lại
    giá trị này để gọi end_run() sau khi xong, dù thành công hay lỗi (nên
    đặt end_run() trong khối finally/except ở loader, không chỉ ở happy path).
    """
    run_id = str(uuid.uuid4())
    with get_session() as session:
        session.execute(
            ingestion_audit_log.insert().values(
                run_id=run_id,
                source_name=source_name,
                table_name=table_name,
                load_mode=load_mode,
                status="running",
                started_at=datetime.now(timezone.utc),
            )
        )
    logger.info(f"[{run_id}] Started {source_name}.{table_name} ({load_mode})")
    return run_id
 
 
def end_run(
    run_id: str,
    status: str,
    row_count: int | None = None,
    error_message: str | None = None,
) -> None:
    """
    Đóng 1 run đã start_run() trước đó. status phải là 'success' hoặc 'failed'.
    """
    if status not in ("success", "failed"):
        raise ValueError("status must be 'success' or 'failed'")
 
    with get_session() as session:
        session.execute(
            ingestion_audit_log.update()
            .where(ingestion_audit_log.c.run_id == run_id)
            .values(
                status=status,
                row_count=row_count,
                error_message=error_message,
                finished_at=datetime.now(timezone.utc),
            )
        )
 
    if status == "success":
        logger.info(f"[{run_id}] Finished — {row_count} rows loaded")
    else:
        logger.error(f"[{run_id}] Failed — {error_message}")
 
 
def get_last_run_status(source_name: str, table_name: str) -> str | None:
    """
    Lấy status của lần chạy gần nhất cho 1 (source, table) — dùng để loader
    tự kiểm tra "lần trước có chạy xong chưa" trước khi chạy tiếp, tránh
    chạy chồng (Prefect concurrency check ở mức ứng dụng, bổ sung cho
    concurrency limit của chính Prefect).
    """
    with get_session() as session:
        result = session.execute(
            ingestion_audit_log.select()
            .where(
                ingestion_audit_log.c.source_name == source_name,
                ingestion_audit_log.c.table_name == table_name,
            )
            .order_by(ingestion_audit_log.c.started_at.desc())
            .limit(1)
        ).first()
    return result.status if result else None