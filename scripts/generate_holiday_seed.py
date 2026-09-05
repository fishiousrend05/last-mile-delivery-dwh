"""
scripts/generate_holiday_seed.py — Gọi Nager.Date API 1 LẦN để sinh
dbt/seeds/holidays.csv.
 
THIẾT KẾ LẠI (v2) — khác bản đầu ở chỗ: KHÔNG chỉ gọi API nữa. Giờ còn đọc
thêm olist_orders_dataset.csv để tính holiday_impact_tier dựa trên % chênh
lệch order_count thực tế so với baseline ngày thường (xem
scripts/event_impact_analysis.py — module dùng chung với
generate_commercial_events_seed.py).
 
Vẫn là SCRIPT ONE-OFF (không thuộc Prefect flow định kỳ) — output đi THẲNG
vào dbt/seeds/holidays.csv, KHÔNG qua postgres_loader, KHÔNG vào landing schema.
 
Input:  Nager.Date public API (https://date.nager.at)
        data/source/olist/olist_orders_dataset.csv (để tính baseline/tier)
Output: dbt/seeds/holidays.csv
        Cột: date, local_name, name, country_code, holiday_impact_tier
        (khớp holidays_schema trong ingestion/schemas/holiday_schema.py)
 
LƯU Ý QUAN TRỌNG: chạy script này TRƯỚC generate_commercial_events_seed.py —
script kia đọc lại dbt/seeds/holidays.csv để loại các ngày lễ khỏi baseline
"Normal_Day" khi tính commercial_event_tier.
"""
 
from __future__ import annotations
 
import time
from pathlib import Path
 
import pandas as pd
import requests
 
from ingestion.schemas.holiday_schema import holidays_schema
from ingestion.utils.logger import get_logger
from ingestion.validators.api_validator import ApiValidationError, validate_response
from scripts.event_impact_analysis import (
    DEFAULT_ORDERS_PATH,
    compute_event_impact,
    load_daily_order_counts,
)
 
logger = get_logger(__name__)
 
NAGER_BASE_URL = "https://date.nager.at/api/v3/PublicHolidays"
COUNTRY_CODE = "BR"
YEARS = [2016, 2017, 2018]  # khớp date range Olist (~2016-09 -> 2018-10)
DEFAULT_OUTPUT_PATH = Path("dbt/seeds/holidays.csv")
 
# Retry cục bộ — cùng triết lý với weather_extractor.py (lỗi tạm thời tự
# đứng lên tại chỗ), dù ở đây chỉ có 3 request (1/năm) nên rủi ro thấp hơn
# nhiều so với ~6800 request của weather.
MAX_RETRIES = 3
RETRY_BACKOFF_BASE_SECONDS = 2.0
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
 
 
def _fetch_year(year: int, session: requests.Session | None = None) -> list[dict]:
    """Gọi Nager.Date cho 1 năm, tự retry nếu lỗi tạm thời (mạng/rate limit)."""
    http_get = session.get if session is not None else requests.get
    url = f"{NAGER_BASE_URL}/{year}/{COUNTRY_CODE}"
 
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = http_get(url, timeout=30)
        except requests.exceptions.RequestException as e:
            last_error = e
            logger.warning(f"[{year}] Network error on attempt {attempt}/{MAX_RETRIES}: {e}")
        else:
            if response.status_code == 200:
                return validate_response(response, source_name="nager-date")
 
            if response.status_code in RETRYABLE_STATUS_CODES:
                last_error = ApiValidationError(f"Retryable status {response.status_code}")
                logger.warning(
                    f"[{year}] Retryable status {response.status_code} "
                    f"on attempt {attempt}/{MAX_RETRIES}"
                )
            else:
                validate_response(response, source_name="nager-date")
 
        if attempt < MAX_RETRIES:
            backoff = RETRY_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
            logger.info(f"[{year}] Retrying in {backoff:.0f}s...")
            time.sleep(backoff)
 
    raise RuntimeError(
        f"Failed to fetch holidays for year {year} after {MAX_RETRIES} attempt(s): {last_error}"
    )
 
 
def fetch_raw_holidays(
    years: list[int] = YEARS, session: requests.Session | None = None
) -> pd.DataFrame:
    """Gọi Nager.Date cho từng năm, reshape thành DataFrame (date, local_name, name, country_code)."""
    records = []
    for year in years:
        logger.info(f"Fetching holidays for {COUNTRY_CODE} {year}...")
        holidays = _fetch_year(year, session=session)
        for h in holidays:
            records.append(
                {
                    "date": h["date"],
                    "local_name": h["localName"],
                    "name": h["name"],
                    "country_code": h["countryCode"],
                }
            )
 
    df = pd.DataFrame(records)
    logger.info(f"Fetched {len(df)} holiday(s) across {len(years)} year(s)")
    return df
 
 
def main(
    output_path: Path = DEFAULT_OUTPUT_PATH,
    orders_path: Path = DEFAULT_ORDERS_PATH,
    years: list[int] = YEARS,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    df = fetch_raw_holidays(years=years, session=session)
    df["date"] = pd.to_datetime(df["date"])
 
    # Tính holiday_impact_tier từ order_count thực tế — DERIVED, không có
    # sẵn trong response Nager.Date.
    daily_orders = load_daily_order_counts(orders_path)
    baseline_avg, holiday_tier_by_name, _, overlap_dates = compute_event_impact(
        daily_orders, df[["date", "local_name"]]
    )
    df["holiday_impact_tier"] = df["local_name"].map(holiday_tier_by_name).fillna(
        "Tier_3_Neutral"
    )
 
    if overlap_dates:
        logger.warning(
            f"{len(overlap_dates)} date(s) overlap with commercial events — "
            f"review COMMERCIAL_EVENTS in scripts/event_impact_analysis.py"
        )
 
    # Validate NGAY trước khi ghi seed — seed sai sẽ làm dbt build ra dim_date
    # sai mà không có cảnh báo rõ ràng nào, nên chặn lỗi ở đây, không để lọt
    # xuống tận dbt mới phát hiện.
    validated_df = holidays_schema.validate(df, lazy=True)
 
    output_path.parent.mkdir(parents=True, exist_ok=True)
    validated_df.to_csv(output_path, index=False)
    logger.info(
        f"Saved holiday seed to {output_path} ({len(validated_df)} row(s), "
        f"baseline={baseline_avg:.0f} orders/day)"
    )
 
    return validated_df
 
 
if __name__ == "__main__":
    main()