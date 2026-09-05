"""
ingestion/schemas/commercial_event_schema.py — Schema cho
dbt/seeds/commercial_events.csv.
 
Track HOÀN TOÀN ĐỘC LẬP với holiday_schema.py — is_commercial_event /
commercial_event_name / commercial_event_tier không đến từ API nào cả, mà
từ lịch tự định nghĩa (COMMERCIAL_EVENTS trong scripts/event_impact_analysis.py),
dựa trên domain research + quét đỉnh xu hướng order thực tế.
 
STM classification: commercial_event_name = DERIVED (tự định nghĩa khung
ngày); commercial_event_tier = DERIVED (tính từ order_count thực tế, cùng
công thức % vs baseline như holiday_impact_tier nhưng KHÔNG gộp qua các
năm — mỗi năm 1 tier riêng, xem event_impact_analysis.py).
 
is_commercial_event KHÔNG có trong bảng này — SUY RA ở tầng dbt khi LEFT
JOIN bảng này vào full date spine của dim_date, giống hệt cách xử lý
holidays.csv.
"""
 
import pandera as pa
from pandera import Column, Check
 
# PHẢI khớp chính xác holiday_schema.IMPACT_TIERS — dùng chung 1 thang đo.
IMPACT_TIERS = [
    "Tier_1_Mega_Boost",
    "Tier_2_High_Boost",
    "Tier_3_Neutral",
    "Tier_4_Mild_Drop",
    "Tier_5_Severe_Drop",
]
 
commercial_events_schema = pa.DataFrameSchema(
    {
        "date": Column(pa.DateTime, nullable=False),
        # tên nhóm sự kiện — CÓ năm trong tên (vd 'Mothers_Day_Peak_2017'),
        # khác holiday_name (không năm) — vì tier tính riêng theo từng năm.
        "commercial_event_name": Column(str, nullable=False),
        "commercial_event_tier": Column(str, Check.isin(IMPACT_TIERS), nullable=False),
    },
    # 1 ngày chỉ thuộc về đúng 1 nhóm sự kiện — các khung Peak/Cutoff được
    # thiết kế liền kề nhau, không chồng lấn (xem COMMERCIAL_EVENTS).
    checks=Check(
        lambda df: ~df.duplicated(subset=["date"]).any(),
        error="Duplicate date found across commercial events — check for overlapping date ranges in COMMERCIAL_EVENTS",
    ),
    strict=True,
    coerce=True,
)
 
 
SCHEMAS = {
    "commercial_events": commercial_events_schema,
}