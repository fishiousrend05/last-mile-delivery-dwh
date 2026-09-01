import pandera as pa
from pandera import Column, Check
 
 
weather_daily_schema = pa.DataFrameSchema(
    {
        "date": Column(pa.DateTime, nullable=False),
        # zone_id là H3 index (resolution 5), dạng hex string, độ dài cố định
        "zone_id": Column(str, Check.str_length(15, 15), nullable=False),
        "temp_avg_c": Column(float, Check.in_range(-10, 50), nullable=True),
        "temp_min_c": Column(float, Check.in_range(-15, 45), nullable=True),
        "temp_max_c": Column(float, Check.in_range(-10, 55), nullable=True),
        "precipitation_mm": Column(float, Check.ge(0), nullable=True),
        # nguồn gốc bản ghi, hữu ích để audit khi có API downtime -> fallback null
        "source": Column(str, Check.isin(["open-meteo"]), nullable=False),
    },
    # Check tầng DataFrame (không phải theo cột): mỗi cặp (zone_id, date) chỉ
    # được xuất hiện đúng 1 lần, tránh duplicate do gọi API lặp / retry lỗi.
    checks=Check(
        lambda df: ~df.duplicated(subset=["zone_id", "date"]).any(),
        error="Duplicate (zone_id, date) pair found in weather_daily",
    ),
    # strict=True ở đây vì đây là dữ liệu do chính pipeline reshape ra
    # (không phải CSV ngoài, không có nguy cơ Open-Meteo tự thêm cột lạ vào
    # DataFrame nội bộ của mình) -> nên chặt hơn để bắt lỗi code reshape sai.
    strict=True,
    coerce=True,
)
 
 
SCHEMAS = {
    "weather_daily": weather_daily_schema,
}