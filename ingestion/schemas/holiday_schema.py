"""
ingestion/schemas/holiday_schema.py — Schema cho dbt/seeds/holidays.csv.
 
THIẾT KẾ LẠI (v2) — thay đổi so với bản đầu:
    TRƯỚC: holiday_type (national/regional/optional) — phân loại TĨNH dựa
           trên field 'fixed'/'global'/'types' của Nager.Date.
    NAY:   holiday_impact_tier — phân loại ĐỘNG dựa trên % chênh lệch
           order_count thực tế so với baseline ngày thường (xem
           scripts/event_impact_analysis.py). Bỏ hẳn is_fixed/is_global vì
           không còn dùng để phân loại nữa.
 
STM classification: date/local_name/name/country_code = REAL (từ Nager.Date
nguyên bản); holiday_impact_tier = DERIVED (tính từ order_count thực tế).
 
is_public_holiday KHÔNG có trong bảng này — nó được SUY RA ở tầng dbt khi
LEFT JOIN bảng này vào full date spine của dim_date (date có mặt trong seed
này => is_public_holiday=True). Bảng này chỉ chứa các ngày CÓ lễ, không phải
toàn bộ lịch.
"""
 
import pandera as pa
from pandera import Column, Check
 
# 5 tier — PHẢI khớp chính xác với assign_impact_tier() trong
# scripts/event_impact_analysis.py (dùng chung 1 thang đo cho cả holiday
# lẫn commercial event).
IMPACT_TIERS = [
    "Tier_1_Mega_Boost",
    "Tier_2_High_Boost",
    "Tier_3_Neutral",
    "Tier_4_Mild_Drop",
    "Tier_5_Severe_Drop",
]
 
holidays_schema = pa.DataFrameSchema(
    {
        "date": Column(pa.DateTime, nullable=False),
        # tên ngày lễ theo tiếng địa phương (Nager.Date field: localName)
        "local_name": Column(str, nullable=False),
        # tên tiếng Anh (Nager.Date field: name) — giữ lại để tham khảo/debug
        "name": Column(str, nullable=False),
        "country_code": Column(str, Check.isin(["BR"]), nullable=False),
        # DERIVED — tính từ % chênh lệch order_count so với baseline. Holiday
        # ngoài CLEAN_WINDOW (2017-01 -> 2018-08) mặc định 'Tier_3_Neutral'
        # vì không đủ dữ liệu tin cậy để tính riêng.
        "holiday_impact_tier": Column(str, Check.isin(IMPACT_TIERS), nullable=False),
    },
    # Mỗi ngày chỉ có tối đa 1 ngày lễ (đã xác nhận Brazil không có same-day
    # multiple holidays) -> date phải unique trong phạm vi 1 country_code.
    checks=Check(
        lambda df: ~df.duplicated(subset=["date", "country_code"]).any(),
        error="Duplicate holiday found for the same date and country_code",
    ),
    strict=True,
    coerce=True,
)
 
 
SCHEMAS = {
    "holidays": holidays_schema,
}