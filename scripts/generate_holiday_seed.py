"""
scripts/generate_holiday_seed.py — Gọi Nager.Date API 1 LẦN để sinh
dbt/seeds/holidays.csv.
 
QUAN TRỌNG: đây là SCRIPT ONE-OFF, KHÔNG phải extractor chạy định kỳ trong
Prefect flow (khác weather_extractor.py — thành phần pipeline thật). Theo
quyết định đã chốt: dim_holiday đã merge vào dim_date, Brazil chỉ cần 3 năm
holiday cố định (2016-2018) — không có lý do gì để gọi lại API này định kỳ.
 
Vì vậy: KHÔNG đi qua ingestion/extractors/, KHÔNG qua postgres_loader, KHÔNG
ghi vào landing schema — output đi THẲNG vào dbt/seeds/holidays.csv, dbt tự
load seed này vào Postgres qua lệnh `dbt seed`.
 
Input:  Nager.Date public API (https://date.nager.at) — không cần API key.
Output: dbt/seeds/holidays.csv
        Cột: date, local_name, name, country_code, is_fixed, is_global,
        holiday_type — khớp đúng holidays_schema trong
        ingestion/schemas/holiday_schema.py.
"""
 
from __future__ import annotations
 
import time
from pathlib import Path
 
import pandas as pd
import requests
 
from ingestion.schemas.holiday_schema import holidays_schema
from ingestion.utils.logger import get_logger
from ingestion.validators.api_validator import ApiValidationError, validate_response
 
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
 
 
def _classify_holiday_type(holiday: dict) -> str:
    """
    Map field 'types'/'global' của Nager.Date sang holiday_type tự định nghĩa
    (national/regional/optional) — cột DERIVED, KHÔNG có sẵn trong response gốc.
    """
    types = holiday.get("types", [])
    if "Optional" in types:
        return "optional"
    if holiday.get("global") is True:
        return "national"
    return "regional"  # global=False nghĩa là chỉ áp dụng ở 1 số bang (counties)
 
 
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
                # Lỗi không tạm thời (vd 404 sai country code) -> raise ngay,
                # không phí lượt retry.
                validate_response(response, source_name="nager-date")
 
        if attempt < MAX_RETRIES:
            backoff = RETRY_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
            logger.info(f"[{year}] Retrying in {backoff:.0f}s...")
            time.sleep(backoff)
 
    raise RuntimeError(
        f"Failed to fetch holidays for year {year} after {MAX_RETRIES} attempt(s): {last_error}"
    )
 
 
def build_holiday_table(
    years: list[int] = YEARS, session: requests.Session | None = None
) -> pd.DataFrame:
    """Gọi Nager.Date cho từng năm, gộp + reshape thành DataFrame khớp holidays_schema."""
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
                    "is_fixed": h["fixed"],
                    "is_global": h["global"],
                    "holiday_type": _classify_holiday_type(h),
                }
            )
 
    df = pd.DataFrame(records)
    logger.info(f"Fetched {len(df)} holiday(s) across {len(years)} year(s)")
    return df
 
 
def main(
    output_path: Path = DEFAULT_OUTPUT_PATH,
    years: list[int] = YEARS,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    df = build_holiday_table(years=years, session=session)
 
    # Validate NGAY trước khi ghi seed — seed sai sẽ làm dbt build ra dim_date
    # sai mà không có cảnh báo rõ ràng nào, nên chặn lỗi ở đây, không để lọt
    # xuống tận dbt mới phát hiện.
    validated_df = holidays_schema.validate(df, lazy=True)
 
    output_path.parent.mkdir(parents=True, exist_ok=True)
    validated_df.to_csv(output_path, index=False)
    logger.info(f"Saved holiday seed to {output_path} ({len(validated_df)} row(s))")
 
    return validated_df
 
 
if __name__ == "__main__":
    main()