#Đóng vai trò là "người gác cổng". Nó sẽ nhận dữ liệu từ Extractor, đối chiếu với source_schema.yaml để đảm bảo không có cột nào bị đổi tên hay sai kiểu dữ liệu trước khi đi tiếp.
"""
validators/schema_validator.py — Generic runner gọi vào các Pandera schema
đã định nghĩa trong ingestion/schemas/*.py.
 
Nhiệm vụ DUY NHẤT của module này: nhận 1 DataFrame + tên bảng, validate theo
đúng schema tương ứng, trả về kết quả có cấu trúc để extractor/loader tự
quyết định làm gì tiếp (load tiếp, quarantine, hay fail cả flow) — module
này KHÔNG tự quyết định, KHÔNG tự ghi vào ingestion_audit_log (việc đó là
của loader, vì audit log gắn với 1 lần Extract-Load hoàn chỉnh, không phải
riêng bước validate).
"""
 
from __future__ import annotations
 
import pandas as pd
import pandera as pa
 
from ingestion.schemas import holiday_schema, olist_schemas, synthetic_schemas, weather_schema
from ingestion.utils.logger import get_logger
 
logger = get_logger(__name__)
 
 
def _build_registry() -> dict[str, pa.DataFrameSchema]:
    """
    Gộp SCHEMAS dict từ cả 4 module thành 1 registry duy nhất. Raise ngay
    lúc import nếu có 2 module lỡ đặt trùng tên bảng — lỗi cấu hình nên bắt
    sớm lúc load module, không để lọt tới lúc validate() rồi mới phát hiện
    validate nhầm schema.
    """
    registry: dict[str, pa.DataFrameSchema] = {}
    for module in (olist_schemas, weather_schema, holiday_schema, synthetic_schemas):
        for table_name, schema in module.SCHEMAS.items():
            if table_name in registry:
                raise ValueError(
                    f"Duplicate table_name '{table_name}' registered in both "
                    f"schema modules — check ingestion/schemas/*.py for a naming collision."
                )
            registry[table_name] = schema
    return registry
 
 
SCHEMA_REGISTRY: dict[str, pa.DataFrameSchema] = _build_registry()
 
 
def list_registered_tables() -> list[str]:
    """Danh sách tên bảng đang có schema đăng ký — hữu ích cho unit test và debug."""
    return sorted(SCHEMA_REGISTRY.keys())
 
 
def validate(
    df: pd.DataFrame, table_name: str
) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """
    Validate `df` theo schema của `table_name`.
 
    Dùng lazy=True — Pandera gom TẤT CẢ lỗi trong 1 lần chạy thay vì dừng ở
    lỗi đầu tiên, để extractor thấy được toàn bộ vấn đề của file ngay, không
    phải sửa-chạy lại nhiều vòng.
 
    Returns:
        (validated_df, None)      nếu validate thành công — validated_df đã
                                   được coerce kiểu dữ liệu theo schema.
        (None, failure_cases_df)  nếu có lỗi — failure_cases_df là DataFrame
                                   liệt kê từng dòng/cột/lý do lỗi cụ thể,
                                   extractor/loader tự quyết định log ra file,
                                   quarantine, hay raise tiếp.
 
    Raises:
        KeyError nếu table_name chưa được đăng ký schema nào — đây LÀ lỗi
        cấu hình (thiếu schema), khác với lỗi dữ liệu, nên raise thẳng chứ
        không trả về tuple như lỗi dữ liệu thông thường.
    """
    if table_name not in SCHEMA_REGISTRY:
        raise KeyError(
            f"No schema registered for '{table_name}'. "
            f"Registered tables: {list_registered_tables()}"
        )
 
    schema = SCHEMA_REGISTRY[table_name]
    logger.info(f"Validating '{table_name}' — {len(df)} rows")
 
    try:
        validated_df = schema.validate(df, lazy=True)
        logger.info(f"Validation PASSED for '{table_name}' — {len(validated_df)} rows")
        return validated_df, None
    except pa.errors.SchemaErrors as e:
        failure_cases = e.failure_cases
        n_failures = len(failure_cases)
        n_affected_rows = failure_cases["index"].nunique() if "index" in failure_cases else n_failures
        logger.error(
            f"Validation FAILED for '{table_name}' — "
            f"{n_failures} failure case(s) across {n_affected_rows} row(s)"
        )
        return None, failure_cases
 
 
def validate_or_raise(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Bản "cứng" của validate() — dùng khi extractor muốn fail ngay lập tức
    thay vì tự xử lý tuple (vd Olist: dữ liệu tĩnh, lỗi nghĩa là có gì đó
    sai nghiêm trọng, không có lý do gì để load tiếp).
 
    Với các nguồn có thể chấp nhận quarantine từng phần (vd synthetic data,
    nơi 1 vài attempt lỗi không nên chặn cả batch), dùng validate() ở trên
    và tự xử lý failure_cases thay vì hàm này.
    """
    validated_df, failure_cases = validate(df, table_name)
    if failure_cases is not None:
        raise ValueError(
            f"Schema validation failed for '{table_name}' "
            f"({len(failure_cases)} failure case(s)). "
            f"Sample:\n{failure_cases.head(10)}"
        )
    return validated_df