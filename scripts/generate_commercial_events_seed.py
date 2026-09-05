"""
scripts/generate_commercial_events_seed.py — Sinh dbt/seeds/commercial_events.csv
từ lịch sự kiện thương mại tự định nghĩa (COMMERCIAL_EVENTS trong
scripts/event_impact_analysis.py).
 
Track HOÀN TOÀN ĐỘC LẬP với holiday (không gọi API nào cả — toàn bộ khung
ngày đã được tự định nghĩa sẵn từ domain research + quét đỉnh xu hướng order
thực tế trong event_type.ipynb).
 
SCRIPT ONE-OFF — output đi THẲNG vào dbt/seeds/commercial_events.csv, KHÔNG
qua postgres_loader, KHÔNG vào landing schema.
 
QUAN TRỌNG: chạy script này SAU generate_holiday_seed.py — cần đọc lại
dbt/seeds/holidays.csv (đã sinh trước đó) để loại đúng các ngày lễ khỏi
baseline "Normal_Day" khi tính commercial_event_tier — dùng CHUNG 1
baseline_avg với holiday_impact_tier (xem event_impact_analysis.py).
 
Input:  dbt/seeds/holidays.csv (từ generate_holiday_seed.py)
        data/source/olist/olist_orders_dataset.csv
Output: dbt/seeds/commercial_events.csv
        Cột: date, commercial_event_name, commercial_event_tier
        (khớp commercial_events_schema trong
        ingestion/schemas/commercial_event_schema.py)
"""
 
from __future__ import annotations
 
from pathlib import Path
 
import pandas as pd
 
from ingestion.schemas.commercial_event_schema import commercial_events_schema
from ingestion.utils.logger import get_logger
from ingestion.validators.file_validator import validate_file
from scripts.event_impact_analysis import (
    COMMERCIAL_EVENTS,
    DEFAULT_ORDERS_PATH,
    compute_event_impact,
    load_daily_order_counts,
)
 
logger = get_logger(__name__)
 
DEFAULT_HOLIDAYS_SEED_PATH = Path("dbt/seeds/holidays.csv")
DEFAULT_OUTPUT_PATH = Path("dbt/seeds/commercial_events.csv")
 
 
def load_holidays_seed(path: str | Path = DEFAULT_HOLIDAYS_SEED_PATH) -> pd.DataFrame:
    """
    Đọc lại dbt/seeds/holidays.csv đã sinh trước đó (KHÔNG gọi lại Nager.Date)
    — chỉ cần cột date/local_name để tính baseline chính xác, giống hệt
    generate_holiday_seed.py đã dùng.
    """
    validated_path = validate_file(path, expected_extension=".csv")
    df = pd.read_csv(validated_path)
    if "date" not in df.columns or "local_name" not in df.columns:
        raise ValueError(
            f"'{validated_path}' missing required column(s). "
            f"Re-run scripts/generate_holiday_seed.py first?"
        )
    logger.info(f"Loaded {len(df):,} holiday(s) from {validated_path} for baseline calculation")
    return df
 
 
def build_commercial_events_table(
    event_tier_by_key: dict[str, str],
) -> pd.DataFrame:
    """Expand COMMERCIAL_EVENTS dict thành 1 dòng / ngày, gắn tier đã tính sẵn."""
    records = []
    for event_name, date_range in COMMERCIAL_EVENTS.items():
        tier = event_tier_by_key.get(event_name, "Tier_3_Neutral")
        for d in date_range:
            records.append(
                {
                    "date": d,
                    "commercial_event_name": event_name,
                    "commercial_event_tier": tier,
                }
            )
    df = pd.DataFrame(records)
    logger.info(
        f"Expanded {len(COMMERCIAL_EVENTS)} event group(s) into {len(df):,} date row(s)"
    )
    return df
 
 
def main(
    output_path: Path = DEFAULT_OUTPUT_PATH,
    holidays_seed_path: Path = DEFAULT_HOLIDAYS_SEED_PATH,
    orders_path: Path = DEFAULT_ORDERS_PATH,
) -> pd.DataFrame:
    holidays_df = load_holidays_seed(holidays_seed_path)
    daily_orders = load_daily_order_counts(orders_path)
 
    baseline_avg, _, event_tier_by_key, overlap_dates = compute_event_impact(
        daily_orders, holidays_df
    )
 
    df = build_commercial_events_table(event_tier_by_key)
    df["date"] = pd.to_datetime(df["date"])
 
    if overlap_dates:
        logger.warning(
            f"{len(overlap_dates)} date(s) overlap with public holidays — "
            f"review COMMERCIAL_EVENTS in scripts/event_impact_analysis.py"
        )
 
    validated_df = commercial_events_schema.validate(df, lazy=True)
 
    output_path.parent.mkdir(parents=True, exist_ok=True)
    validated_df.to_csv(output_path, index=False)
    logger.info(
        f"Saved commercial events seed to {output_path} ({len(validated_df)} row(s), "
        f"baseline={baseline_avg:.0f} orders/day)"
    )
 
    return validated_df
 
 
if __name__ == "__main__":
    main()