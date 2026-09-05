#Đọc các file CSV từ data/source/olist/ bằng Pandas hoặc PyArrow. Nó có thể được thiết kế để đọc theo chunk (từng phần nhỏ) để tránh tràn RAM.
"""
extractors/olist_extractor.py — Đọc 9 CSV tĩnh của Olist từ data/source/olist/.
 
Hợp đồng của Extractor trong pipeline:
    Extractor (module này)           -> pd.DataFrame THÔ + metadata nhỏ
    Validator (schema_validator.py)  -> pd.DataFrame SẠCH (ép kiểu, lọc lỗi)
    Loader (postgres_loader.py)      -> ghi vào Postgres
 
Module này CHỈ làm việc của Extractor:
  - validate_file() (vòng ngoài, file_validator.py) TRƯỚC khi đọc vào RAM —
    đây vẫn là việc của Extractor, vì bản chất là 1 phần của "đọc từ nguồn".
  - pd.read_csv() để đưa dữ liệu vào RAM dạng thô.
  - KHÔNG ép kiểu, KHÔNG lọc dòng lỗi, KHÔNG gọi schema_validator — đó là
    bước tiếp theo, do ingestion/flows/ingestion_flow.py điều phối.
"""
 
from __future__ import annotations
 
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
 
import pandas as pd
import yaml
 
from ingestion.extractors.base import ExtractionResult
from ingestion.utils.logger import get_logger
from ingestion.validators.file_validator import FileValidationError, validate_file
 
logger = get_logger(__name__)
 
_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "ingestion_config.yaml"
 
# Mapping table_name (PHẢI khớp tên đăng ký trong schemas/olist_schemas.py
# SCHEMA_REGISTRY) -> tên file thật trên Kaggle. Đặt cứng ở đây vì đây là
# thông tin CỐ ĐỊNH của bản thân dataset Olist — khác với source_dir (có thể
# đổi giữa các môi trường), nên KHÔNG đưa vào ingestion_config.yaml.
OLIST_FILE_MAP: dict[str, str] = {
    "olist_orders": "olist_orders_dataset.csv",
    "olist_order_items": "olist_order_items_dataset.csv",
    "olist_customers": "olist_customers_dataset.csv",
    "olist_products": "olist_products_dataset.csv",
    "olist_sellers": "olist_sellers_dataset.csv",
    "olist_order_payments": "olist_order_payments_dataset.csv",
    "olist_order_reviews": "olist_order_reviews_dataset.csv",
    "olist_geolocation": "olist_geolocation_dataset.csv",
    "product_category_translation": "product_category_name_translation.csv",
}
 
 
def _load_source_dir() -> Path:
    """Đọc source_dir của Olist từ ingestion_config.yaml; fallback nếu thiếu config."""
    if _CONFIG_PATH.exists():
        with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
        source_dir = config.get("sources", {}).get("olist", {}).get("source_dir")
        if source_dir:
            return Path(source_dir)
    logger.warning(
        f"Could not read sources.olist.source_dir from {_CONFIG_PATH}, "
        f"falling back to default 'data/source/olist'"
    )
    return Path("data/source/olist")
 
 
def extract_table(table_name: str, source_dir: str | Path | None = None) -> ExtractionResult:
    """
    Extract 1 bảng Olist theo tên (phải có trong OLIST_FILE_MAP).
 
    Raises:
        KeyError nếu table_name không có trong OLIST_FILE_MAP — lỗi cấu hình,
            không phải lỗi dữ liệu, nên raise thẳng chứ không trả kết quả rỗng.
        FileValidationError nếu file không tồn tại/rỗng/sai định dạng/hỏng
            encoding — bắt được TRƯỚC khi tốn công gọi pd.read_csv().
    """
    if table_name not in OLIST_FILE_MAP:
        raise KeyError(
            f"Unknown Olist table_name '{table_name}'. "
            f"Known tables: {sorted(OLIST_FILE_MAP.keys())}"
        )
 
    source_dir = Path(source_dir) if source_dir else _load_source_dir()
    file_path = source_dir / OLIST_FILE_MAP[table_name]
 
    validated_path = validate_file(file_path, expected_extension=".csv")
 
    logger.info(f"Reading '{table_name}' from {validated_path}")
    df = pd.read_csv(validated_path)
 
    metadata = {
        "source_name": "olist",
        "table_name": table_name,
        "source_file": str(validated_path),
        "row_count_raw": len(df),
        "extracted_at": datetime.now(timezone.utc).isoformat(),
    }
 
    logger.info(f"Extracted '{table_name}' — {len(df):,} raw rows")
    return ExtractionResult(table_name=table_name, df=df, metadata=metadata)
 
 
def extract_all(source_dir: str | Path | None = None) -> list[ExtractionResult]:
    """
    Extract toàn bộ 9 bảng Olist.
 
    KHÔNG fail-fast toàn bộ nếu 1 bảng lỗi — log lỗi và tiếp tục các bảng
    còn lại, để 1 file hỏng/thiếu không chặn luôn 8 bảng kia (hữu ích lúc
    dev khi có thể chưa tải đủ hết CSV). Bảng lỗi sẽ KHÔNG có mặt trong danh
    sách trả về — nếu flow cần strict (bắt buộc đủ 9 bảng), tự so sánh
    `len(results)` với `len(OLIST_FILE_MAP)` ở nơi gọi hàm này.
    """
    results: list[ExtractionResult] = []
    for table_name in OLIST_FILE_MAP:
        try:
            results.append(extract_table(table_name, source_dir=source_dir))
        except (FileValidationError, KeyError) as e:
            logger.error(f"Failed to extract '{table_name}': {e}")
    return results