"""
extractors/base.py — Định nghĩa dùng chung cho MỌI extractor trong pipeline.
 
Hợp đồng (contract) đã thống nhất:
    Extractor  -> ExtractionResult (pd.DataFrame THÔ + metadata nhỏ)
    Validator  -> pd.DataFrame SẠCH (ép kiểu, lọc lỗi)
    Loader     -> ghi vào Postgres
 
Đặt ở đây (thay vì định nghĩa lặp lại trong từng extractor) để olist_extractor.py,
weather_extractor.py, holiday_extractor.py đều trả về đúng 1 kiểu dữ liệu,
tránh lệch pha khi ingestion_flow.py ráp nối các extractor lại với nhau.
"""
 
from __future__ import annotations
 
from dataclasses import dataclass, field
 
import pandas as pd
 
 
@dataclass
class ExtractionResult:
    """
    Gói kết quả của 1 lần extract 1 bảng: DataFrame thô + metadata nhỏ đi kèm.
 
    Validator (bước sau) chỉ cần `.df`. `.metadata` dùng để log/ghi audit ở
    tầng loader (vd source_file, row_count_raw, extracted_at) — KHÔNG phải
    dữ liệu nghiệp vụ, không đi vào Postgres landing table.
    """
 
    table_name: str
    df: pd.DataFrame
    metadata: dict = field(default_factory=dict)