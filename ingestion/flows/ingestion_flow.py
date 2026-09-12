#flows/ingestion_flow.py: Đây là bộ não trung tâm (thường được viết bằng các framework như Prefect hoặc Airflow). File này không chứa logic xử lý dữ liệu, mà nó chứa logic quản lý quy trình. Nó quyết định thứ tự chạy, xử lý lỗi luồng (nếu API sập thì có chạy tiếp phần CSV không), và gửi cảnh báo.
"""
ingestion/flows/ingestion_flow.py — Nhạc trưởng của Phase 1: Source & Ingestion.

Pipeline:
    Olist CSV
        -> Extract
        -> Schema validation
        -> PostgreSQL landing

    Synthetic
        -> Generate drivers
        -> Generate delivery_attempts (neo Olist + weather đã có sẵn + seeds)
        -> Schema validation
        -> raw/synthetic CSV cache
        -> PostgreSQL landing

Weather KHÔNG còn chạy trong flow này — xem ingestion/flows/weather_ingestion_flow.py
(flow riêng, chạy theo lịch). Lý do tách: Open-Meteo free tier quota rất hẹp
(thực nghiệm ~3 request/70s), fetch đủ ~6800 zone cần nhiều giờ — không nên
chặn Olist/Synthetic mỗi lần chạy full pipeline chỉ vì đang đợi weather.
Flow này chỉ ĐỌC LẠI bất kỳ dữ liệu weather nào đã có sẵn trong
landing.weather_daily tại thời điểm chạy (có thể chưa đầy đủ — synthetic đã
được thiết kế graceful degradation cho trường hợp này).

Design principles
-----------------
- Flow chỉ ORCHESTRATE, không chứa business/data transformation logic.
- Extractor/generator tạo ExtractionResult.
- schema_validator chịu trách nhiệm data contract.
- file_loader chỉ lo local raw cache.
- postgres_loader chỉ lo landing + audit.
- Holidays/commercial_events KHÔNG chạy trong flow; chúng là dbt seeds
  được tạo bởi các one-off scripts đã thống nhất trước đó.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import yaml
from prefect import flow, get_run_logger, task

import pandas as pd
from ingestion.extractors.base import ExtractionResult
from ingestion.extractors import olist_extractor
from ingestion.generators import synthetic_generator
from ingestion.loaders.file_loader import save_to_raw_file
from ingestion.loaders.postgres_loader import get_landing_schema, load_dataframe
from ingestion.utils.database import get_engine, test_connection
from ingestion.validators.schema_validator import validate_or_raise


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FLOW_NAME = "last-mile-phase-1-ingestion"

OLIST_TABLES = tuple(olist_extractor.OLIST_FILE_MAP.keys())

_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "ingestion_config.yaml"

FLOW_TASK_RETRIES = 1
FLOW_TASK_RETRY_DELAY_SECONDS = 30


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _get_load_mode(source_name: str, default: str) -> str:
    """
    Đọc sources.{source_name}.load_mode từ ingestion_config.yaml — để
    load_mode khai báo trong config THỰC SỰ điều khiển hành vi flow, thay vì
    chỉ mang tính mô tả trong khi flow tự hardcode giá trị riêng.
    """
    if _CONFIG_PATH.exists():
        with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
        load_mode = config.get("sources", {}).get(source_name, {}).get("load_mode")
        if load_mode:
            return load_mode
    return default


def _validate_and_load(
    result: ExtractionResult,
    *,
    load_mode: str,
    save_raw: bool = False,
    raw_dir: str | Path | None = None,
) -> tuple[int, pd.DataFrame]:
    """
    Common ExtractResult -> validate -> optional raw cache -> PostgreSQL path.

    Trả về (row_count, validated_df) — validated_df cần thiết cho các bước
    downstream muốn dùng lại bản đã qua schema contract (vd synthetic dùng
    lại olist orders/customers đã validate), thay vì tự đọc lại CSV thô.
    """
    validated_df = validate_or_raise(result.df, result.table_name)

    if save_raw:
        save_to_raw_file(
            validated_df,
            source_name=result.metadata.get("source_name", "unknown"),
            table_name=result.table_name,
            raw_dir=raw_dir,
        )

    row_count = load_dataframe(
        validated_df,
        source_name=result.metadata.get("source_name", "unknown"),
        table_name=result.table_name,
        load_mode=load_mode,
    )
    return row_count, validated_df


def _missing_expected_tables(
    results: Iterable[ExtractionResult],
    expected_tables: Iterable[str],
) -> list[str]:
    """Return expected tables that were not successfully extracted."""
    found = {result.table_name for result in results}
    return [table for table in expected_tables if table not in found]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(
    name="check-postgres-connection",
    retries=FLOW_TASK_RETRIES,
    retry_delay_seconds=FLOW_TASK_RETRY_DELAY_SECONDS,
)
def check_postgres_connection() -> None:
    """
    Fail fast before expensive extraction/API work.

    Nếu Postgres chết, không có lý do gì để tải Olist hoặc gọi hàng nghìn
    request Open-Meteo rồi cuối cùng mới phát hiện không thể load.
    """
    logger = get_run_logger()

    if not test_connection():
        raise ConnectionError(
            "PostgreSQL connection failed. "
            "Check .env and make sure PostgreSQL is running."
        )

    logger.info("PostgreSQL connection check PASSED")


@task(
    name="ingest-olist",
    retries=FLOW_TASK_RETRIES,
    retry_delay_seconds=FLOW_TASK_RETRY_DELAY_SECONDS,
)
def ingest_olist() -> tuple[dict[str, int], dict[str, pd.DataFrame]]:
    """
    Extract + validate + load toàn bộ 9 bảng Olist.

    Olist là nguồn tĩnh và là dependency chính của synthetic delivery_attempts,
    nên flow dùng strict behavior: thiếu bất kỳ bảng nào -> fail task.

    Trả về thêm dict validated_tables (table_name -> DataFrame đã qua
    schema_validator) để ingest_synthetic() dùng lại đúng bản đã validate,
    thay vì tự đọc lại CSV thô — nhất quán với cách ingest_weather() đã xử lý.
    """
    logger = get_run_logger()

    results = olist_extractor.extract_all()

    missing = _missing_expected_tables(results, OLIST_TABLES)
    if missing:
        raise RuntimeError(
            "Olist extraction incomplete. "
            f"Missing table(s): {missing}"
        )

    load_mode = _get_load_mode("olist", default="full_refresh")

    loaded: dict[str, int] = {}
    validated_tables: dict[str, pd.DataFrame] = {}

    for result in results:
        row_count, validated_df = _validate_and_load(
            result,
            load_mode=load_mode,
            save_raw=False,
        )
        loaded[result.table_name] = row_count
        validated_tables[result.table_name] = validated_df

    logger.info(
        "Olist ingestion completed: %d/%d tables, %s total rows",
        len(loaded),
        len(OLIST_TABLES),
        f"{sum(loaded.values()):,}",
    )
    return loaded, validated_tables


@task(
    name="load-weather-from-landing",
    retries=FLOW_TASK_RETRIES,
    retry_delay_seconds=FLOW_TASK_RETRY_DELAY_SECONDS,
)
def load_weather_from_landing() -> pd.DataFrame:
    """
    Đọc landing.weather_daily HIỆN CÓ trong Postgres — KHÔNG gọi Open-Meteo.

    Weather được nạp bởi 1 flow RIÊNG, chạy theo lịch độc lập
    (weather_ingestion_flow.py) — xem docstring đầu module này để hiểu lý do
    tách. Flow chính chỉ đọc lại bất kỳ dữ liệu nào đã có sẵn tại thời điểm
    chạy, dùng làm risk factor cho synthetic.

    Nếu bảng chưa tồn tại (weather_ingestion_flow.py chưa từng chạy lần nào)
    hoặc rỗng, trả về DataFrame rỗng — synthetic_generator đã thiết kế
    graceful degradation cho trường hợp không có weather_df (tắt risk factor
    thời tiết, có log cảnh báo, KHÔNG raise lỗi).
    """
    logger = get_run_logger()
    engine = get_engine()
    schema = get_landing_schema()

    try:
        df = pd.read_sql(f'SELECT * FROM "{schema}"."weather_daily"', engine)
    except Exception as e:
        logger.warning(
            f"Không đọc được {schema}.weather_daily (weather_ingestion_flow.py chưa "
            f"chạy lần nào, hoặc bảng chưa tồn tại?): {e}. Synthetic sẽ chạy KHÔNG có "
            f"weather risk factor."
        )
        return pd.DataFrame(
            columns=["date", "zone_id", "temp_max_c", "temp_min_c", "temp_avg_c", "precipitation_mm", "source"]
        )

    n_zones = df["zone_id"].nunique() if not df.empty else 0
    logger.info(f"Đọc {len(df):,} dòng weather từ landing ({n_zones:,} zone) — dùng làm risk factor cho synthetic.")
    return df


@task(
    name="ingest-synthetic",
    retries=FLOW_TASK_RETRIES,
    retry_delay_seconds=FLOW_TASK_RETRY_DELAY_SECONDS,
)
def ingest_synthetic(
    weather_df: pd.DataFrame, olist_validated: dict[str, pd.DataFrame]
) -> dict[str, int]:
    """
    Generate + validate + load drivers và delivery_attempts.

    Synthetic KHÔNG phải dữ liệu REAL: generator đã được thiết kế để neo vào
    Olist thật và weather thật (dù có thể chưa đầy đủ — xem
    load_weather_from_landing()). Flow chỉ truyền dependency vào generator,
    không tự mô phỏng dữ liệu.

    olist_validated: dict từ ingest_olist() — dùng lại orders/customers/
    geolocation ĐÃ QUA schema_validator, thay vì để synthetic_generator tự
    đọc lại CSV thô.
    """
    logger = get_run_logger()

    drivers_result, attempts_result = synthetic_generator.main(
        orders_df=olist_validated["olist_orders"],
        customers_df=olist_validated["olist_customers"],
        geolocation_df=olist_validated["olist_geolocation"],
        weather_df=weather_df,
    )

    load_mode = _get_load_mode("synthetic", default="full_refresh")
    loaded: dict[str, int] = {}

    for result in (drivers_result, attempts_result):
        validated_df = validate_or_raise(result.df, result.table_name)

        save_to_raw_file(
            validated_df,
            source_name="synthetic",
            table_name=result.table_name,
            raw_dir="data/raw/synthetic",
        )

        row_count = load_dataframe(
            validated_df,
            source_name="synthetic",
            table_name=result.table_name,
            load_mode=load_mode,
        )
        loaded[result.table_name] = row_count

    logger.info(
        "Synthetic ingestion completed: %s",
        {table: f"{rows:,}" for table, rows in loaded.items()},
    )
    return loaded


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

@flow(name=FLOW_NAME, log_prints=False)
def ingestion_flow() -> dict:
    """
    Phase 1 — Source & Ingestion.

    Dependency graph:

        PostgreSQL check
               |
          +----+----+
          |         |
        Olist    Weather
          |         |
          |         v
          |    validated weather
          |         |
          +----+----+
               |
           Synthetic

    Olist và Weather có thể chạy độc lập sau connection check về mặt logic,
    nhưng hiện tại flow chạy tuần tự để:
        1. dễ debug trong giai đoạn development;
        2. tránh tải RAM/API/DB cùng lúc;
        3. giữ log dễ đọc;
        4. không cần Prefect concurrency khi Phase 1 chưa cần scale.
    """
    logger = get_run_logger()

    logger.info("=" * 72)
    logger.info("START %s", FLOW_NAME)
    logger.info("=" * 72)

    # Gate 1: DB phải sống trước khi làm bất kỳ extraction tốn thời gian nào.
    check_postgres_connection()

    # Gate 2: nguồn REAL Olist.
    olist_summary, olist_validated = ingest_olist()

    # Gate 3: weather REAL/DERIVED từ API. Kết quả được truyền trực tiếp
    # sang synthetic để simulation có weather risk thật.
    weather_result = load_weather_from_landing()

    # Gate 4: synthetic data neo vào REAL/DERIVED inputs — dùng lại
    # olist_validated thay vì đọc lại CSV thô.
    synthetic_summary = ingest_synthetic(weather_result, olist_validated)

    summary = {
        "flow": FLOW_NAME,
        "status": "success",
        "olist": olist_summary,
        "weather": {
            "rows": len(weather_result.df),
            "zones_requested": weather_result.metadata.get("zones_requested"),
            "zones_succeeded": weather_result.metadata.get("zones_succeeded"),
            "zones_failed": len(weather_result.metadata.get("zones_failed", [])),
        },
        "synthetic": synthetic_summary,
    }

    logger.info("=" * 72)
    logger.info("PHASE 1 INGESTION COMPLETED")
    logger.info("Summary: %s", summary)
    logger.info("=" * 72)

    return summary


if __name__ == "__main__":
    ingestion_flow()