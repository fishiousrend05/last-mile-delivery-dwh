"""
scripts/event_impact_analysis.py — Module DÙNG CHUNG cho 2 script sinh seed:
    - generate_holiday_seed.py            -> dbt/seeds/holidays.csv
    - generate_commercial_events_seed.py  -> dbt/seeds/commercial_events.csv
 
Chứa 1 NGUỒN SỰ THẬT DUY NHẤT cho:
    - Lịch sự kiện thương mại tự định nghĩa (COMMERCIAL_EVENTS)
    - Cửa sổ "dữ liệu sạch" để tính baseline
    - Hàm phân loại tier (assign_impact_tier)
    - Logic tính baseline_avg + tier cho cả holiday lẫn commercial event
 
Tách riêng module này (thay vì lặp lại trong từng script) để đảm bảo
holiday_impact_tier và commercial_event_tier luôn được tính từ ĐÚNG 1
baseline_avg — tránh 2 script tính ra 2 con số khác nhau nếu sau này sửa
code không đồng bộ giữa 2 nơi.
 
QUAN TRỌNG — cửa sổ tính baseline (CLEAN_WINDOW): 2017-01-01 -> 2018-08-31
    - Trước 01/2017 (giai đoạn Beta): dữ liệu quá ít (vài đơn/ngày), giữ lại
      sẽ kéo tụt baseline, làm sai lệch mọi % so sánh.
    - Sau 31/08/2018 (giai đoạn đứt gãy): dữ liệu tháng 09/2018 Olist bị cắt
      ngang giữa chừng, không đại diện cho 1 tháng đầy đủ.
 
Holiday/event nằm NGOÀI cửa sổ này (vd cuối 2016, đầu 09-10/2018) KHÔNG được
dùng để TÍNH baseline/tier — nhưng holiday vẫn xuất hiện trong seed output
với is_public_holiday=True (đó vẫn là 1 ngày lễ CÓ THẬT), chỉ là
holiday_impact_tier của chúng mặc định về 'Tier_3_Neutral' vì không đủ dữ
liệu tin cậy để tính tier riêng cho chúng.
"""
 
from __future__ import annotations
 
from pathlib import Path
 
import pandas as pd
 
from ingestion.utils.logger import get_logger
from ingestion.validators.file_validator import validate_file
 
logger = get_logger(__name__)
 
CLEAN_WINDOW_START = pd.Timestamp("2017-01-01")
CLEAN_WINDOW_END = pd.Timestamp("2018-08-31")
 
DEFAULT_ORDERS_PATH = Path("data/source/olist/olist_orders_dataset.csv")
 
# Nguồn sự thật DUY NHẤT cho lịch sự kiện thương mại — tự định nghĩa dựa trên
# domain research (Consumer Week, Black Friday) + quét đỉnh xu hướng thực tế
# (Mother's/Valentine's/Father's Day, Christmas). Peak = giai đoạn tăng
# trưởng dẫn tới lễ; Cutoff = giai đoạn hạ nhiệt do khách sợ giao hàng trễ
# không kịp lễ (shipping cutoff effect) — phát hiện qua phân tích cận cảnh
# tháng 12/2017 (Christmas_Leadup ban đầu bắt nhầm đoạn dốc xuống).
#
# Commercial event group theo KEY CÓ NĂM (khác holiday, group theo tên
# KHÔNG năm) — vì hiệu quả từng chiến dịch có thể đổi khác giữa các năm
# (vd Mothers_Day_Leadup_2017 và _2018 cho tier rất khác nhau).
COMMERCIAL_EVENTS: dict[str, pd.DatetimeIndex] = {
    # Black_Friday_Week kết thúc SỚM HƠN dự kiến ban đầu (24/11 thay vì 27/11)
    # để không chồng lấn với Christmas_Shopping_Peak (bắt đầu 25/11) — 3 ngày
    # 25-27/11 thuộc về Christmas_Shopping_Peak, không phải Black Friday.
    "Black_Friday_Week_2017": pd.date_range(start="2017-11-20", end="2017-11-24"),
    "Consumer_Week_2017": pd.date_range(start="2017-03-10", end="2017-03-15"),
    "Consumer_Week_2018": pd.date_range(start="2018-03-10", end="2018-03-15"),
    "Mothers_Day_Peak_2017": pd.date_range(start="2017-04-30", end="2017-05-09"),
    "Mothers_Day_Cutoff_2017": pd.date_range(start="2017-05-10", end="2017-05-14"),
    "Mothers_Day_Peak_2018": pd.date_range(start="2018-04-25", end="2018-05-05"),
    "Mothers_Day_Cutoff_2018": pd.date_range(start="2018-05-06", end="2018-05-13"),
    "Dia_dos_Namorados_Peak_2017": pd.date_range(start="2017-05-28", end="2017-06-05"),
    "Dia_dos_Namorados_Cutoff_2017": pd.date_range(start="2017-06-06", end="2017-06-12"),
    "Dia_dos_Namorados_Peak_2018": pd.date_range(start="2018-05-28", end="2018-06-06"),
    "Dia_dos_Namorados_Cutoff_2018": pd.date_range(start="2018-06-07", end="2018-06-12"),
    "Fathers_Day_Peak_2017": pd.date_range(start="2017-07-25", end="2017-08-04"),
    "Fathers_Day_Cutoff_2017": pd.date_range(start="2017-08-05", end="2017-08-13"),
    "Fathers_Day_Peak_2018": pd.date_range(start="2018-07-25", end="2018-08-05"),
    "Fathers_Day_Cutoff_2018": pd.date_range(start="2018-08-06", end="2018-08-12"),
    "Christmas_Shopping_Peak_2017": pd.date_range(start="2017-11-25", end="2017-12-08"),
    "Pre_Christmas_Shipping_Cutoff_2017": pd.date_range(start="2017-12-10", end="2017-12-24"),
}
 
 
def assign_impact_tier(pct: float) -> str:
    """
    Phân loại tier theo % chênh lệch so với baseline ngày thường — thang đo
    ĐỐI XỨNG dùng chung cho cả holiday_impact_tier và commercial_event_tier
    (ranh giới suy ra từ dữ liệu thực nghiệm: Tier_3 là dải rộng nhất, chứa
    cả 2 dấu +/- quanh baseline).
    """
    if pct >= 50:
        return "Tier_1_Mega_Boost"
    elif 15 <= pct < 50:
        return "Tier_2_High_Boost"
    elif -15 <= pct < 15:
        return "Tier_3_Neutral"
    elif -50 <= pct < -15:
        return "Tier_4_Mild_Drop"
    else:
        return "Tier_5_Severe_Drop"
 
 
def load_daily_order_counts(orders_path: str | Path = DEFAULT_ORDERS_PATH) -> pd.DataFrame:
    """Đọc olist_orders_dataset.csv, gom nhóm theo ngày, đếm số đơn/ngày."""
    validated_path = validate_file(orders_path, expected_extension=".csv")
    orders = pd.read_csv(validated_path)
    orders["order_purchase_timestamp"] = pd.to_datetime(orders["order_purchase_timestamp"])
    orders["order_date"] = orders["order_purchase_timestamp"].dt.normalize()
 
    daily = orders.groupby("order_date").size().reset_index(name="order_count")
    logger.info(f"Loaded daily order counts for {len(daily):,} distinct date(s)")
    return daily
 
 
def _build_commercial_event_lookup() -> dict[pd.Timestamp, str]:
    """date -> tên nhóm sự kiện (dict key) — dùng để gán nhãn VÀ loại khỏi baseline."""
    lookup: dict[pd.Timestamp, str] = {}
    for event_name, date_range in COMMERCIAL_EVENTS.items():
        for d in date_range:
            lookup[pd.Timestamp(d)] = event_name
    return lookup
 
 
def compute_event_impact(
    daily_orders: pd.DataFrame, holidays_df: pd.DataFrame
) -> tuple[float, dict[str, str], dict[str, str], list[pd.Timestamp]]:
    """
    Tính baseline_avg (Normal_Day trong CLEAN_WINDOW) + tier cho TỪNG holiday
    (group theo local_name, gộp qua các năm) và TỪNG commercial event (group
    theo dict key, KHÔNG gộp năm).
 
    Args:
        daily_orders: DataFrame có cột order_date, order_count.
        holidays_df: DataFrame có cột date, local_name (đã ở format schema
            của mình, KHÔNG phải camelCase gốc của Nager.Date).
 
    Returns:
        baseline_avg, holiday_tier_by_name, event_tier_by_key, overlap_dates
        (overlap_dates chỉ để CẢNH BÁO, không tự động loại bỏ gì cả).
    """
    event_lookup = _build_commercial_event_lookup()
    event_dates_set = set(event_lookup.keys())
    holiday_dates_set = set(pd.to_datetime(holidays_df["date"]))
 
    overlap_dates = sorted(event_dates_set & holiday_dates_set)
    if overlap_dates:
        logger.warning(
            f"Found {len(overlap_dates)} date(s) overlapping between public "
            f"holiday and commercial event: {[d.date() for d in overlap_dates]}"
        )
    else:
        logger.info("No overlap between public holidays and commercial events")
 
    clean = daily_orders[
        (daily_orders["order_date"] >= CLEAN_WINDOW_START)
        & (daily_orders["order_date"] <= CLEAN_WINDOW_END)
    ].copy()
 
    clean["is_holiday"] = clean["order_date"].isin(holiday_dates_set)
    clean["event_key"] = clean["order_date"].map(event_lookup)
    clean["is_commercial_event"] = clean["event_key"].notna()
 
    # Normal_Day = KHÔNG phải holiday VÀ KHÔNG phải commercial event.
    normal_days = clean[~clean["is_holiday"] & ~clean["is_commercial_event"]]
    baseline_avg = normal_days["order_count"].mean()
    logger.info(
        f"Baseline (Normal_Day avg, {CLEAN_WINDOW_START.date()} -> "
        f"{CLEAN_WINDOW_END.date()}): {baseline_avg:.0f} orders/day"
    )
 
    # --- Tier commercial event: group theo event_key, KHÔNG gộp năm ---
    event_rows = clean[clean["is_commercial_event"]]
    event_avg = event_rows.groupby("event_key")["order_count"].mean()
    event_tier_by_key = {
        key: assign_impact_tier(((avg - baseline_avg) / baseline_avg) * 100)
        for key, avg in event_avg.items()
    }
 
    # --- Tier holiday: group theo local_name, GỘP qua các năm trong CLEAN_WINDOW ---
    holidays_in_window = holidays_df[
        (pd.to_datetime(holidays_df["date"]) >= CLEAN_WINDOW_START)
        & (pd.to_datetime(holidays_df["date"]) <= CLEAN_WINDOW_END)
    ].copy()
    holidays_in_window["order_date"] = pd.to_datetime(holidays_in_window["date"])
    merged = holidays_in_window.merge(
        clean[["order_date", "order_count", "is_commercial_event"]], on="order_date", how="inner"
    )
    # Ưu tiên Commercial Event > Public Holiday (khớp resolve_day_type() trong
    # notebook gốc) — ngày vừa là holiday vừa là commercial event chỉ tính
    # vào tier của commercial event, KHÔNG tính vào holiday_avg.
    merged = merged[~merged["is_commercial_event"]]
    holiday_avg = merged.groupby("local_name")["order_count"].mean()
    holiday_tier_by_name = {
        name: assign_impact_tier(((avg - baseline_avg) / baseline_avg) * 100)
        for name, avg in holiday_avg.items()
    }
 
    return baseline_avg, holiday_tier_by_name, event_tier_by_key, overlap_dates