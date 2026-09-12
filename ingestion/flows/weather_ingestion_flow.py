"""
ingestion/flows/weather_ingestion_flow.py — Flow RIÊNG cho weather, TÁCH khỏi
ingestion_flow.py chính (Olist + Synthetic).

VÌ SAO TÁCH RIÊNG:
    Open-Meteo free tier có quota rất hẹp (thực nghiệm từ E2E thật: ~3
    request/70s) — fetch đủ ~6800 zone cần nhiều giờ, không nên chặn
    ingest_olist mỗi lần chạy full pipeline chỉ vì đang đợi weather.

    Flow này CHẠY THEO LỊCH (Prefect deployment cron — xem khối __main__),
    mỗi lần chỉ xử lý 1 SỐ LƯỢNG BATCH GIỚI HẠN (max_batches_per_run) rồi
    dừng CÓ CHỦ ĐÍCH (không phải lỗi) — checkpoint tự lo phần resume ở lần
    chạy kế tiếp.

    ingestion_flow.py (chính) KHÔNG còn gọi weather_extractor nữa — nó chỉ
    ĐỌC LẠI bất kỳ dữ liệu weather nào đã có sẵn trong landing.weather_daily
    tại thời điểm chạy (xem load_weather_from_landing() trong ingestion_flow.py).
    Nếu weather chưa fetch xong toàn bộ, synthetic vẫn chạy được — chỉ là
    risk factor thời tiết sẽ thiếu cho zone/ngày chưa có dữ liệu (đã thiết
    kế graceful degradation từ đầu trong synthetic_generator.py).

CÁCH CHẠY:
    - Chạy tay 1 "lát" (xử lý tối đa max_batches_per_run batch rồi dừng):
          python -m ingestion.flows.weather_ingestion_flow

    - Chạy theo lịch (Prefect deployment, mặc định mỗi giờ 1 lần):
          python -m ingestion.flows.weather_ingestion_flow --serve

    - Lặp lại (thủ công hoặc qua lịch) tới khi log báo is_complete=True.
"""

from __future__ import annotations

import sys
from pathlib import Path

from prefect import flow, get_run_logger, task

from ingestion.extractors import weather_extractor
from ingestion.loaders.file_loader import save_to_raw_file
from ingestion.loaders.postgres_loader import load_dataframe
from ingestion.validators.schema_validator import validate_or_raise

FLOW_NAME = "weather-ingestion"

WEATHER_CHECKPOINT_PATH = "data/raw/weather/weather_daily_checkpoint.csv"

# Số batch xử lý MỖI LẦN CHẠY — cố ý giữ NHỎ để mỗi lần chạy theo lịch kết
# thúc nhanh, dễ đoán, không "treo" lâu nếu quota thực tế khắt khe hơn ước
# tính. Với self-throttle (3 request/70s) + batch_size=10, 8 batch ≈ dưới
# 3 phút/lần chạy trong điều kiện thuận lợi.
DEFAULT_MAX_BATCHES_PER_RUN = 8
DEFAULT_BATCH_SIZE = weather_extractor.DEFAULT_BATCH_SIZE


@task(name="ingest-weather-batch", retries=0)
def ingest_weather_batch(
    max_batches_per_run: int = DEFAULT_MAX_BATCHES_PER_RUN,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict:
    """
    Xử lý TỐI ĐA max_batches_per_run batch trong lần chạy này rồi dừng — dừng
    vì chạm giới hạn KHÔNG phải lỗi (metadata["is_complete"]=False, task vẫn
    Completed bình thường).

    retries=0: batch-level retry + self-throttle + RateLimitExhaustedError
    đã tự xử lý đầy đủ bên trong weather_extractor rồi. Nếu task này vẫn
    fail, đó là dấu hiệu THẬT SỰ nghiêm trọng (không phải rate limit thông
    thường) — để lần chạy theo lịch kế tiếp tự thử lại, không cần Prefect
    retry chồng thêm 1 lớp nữa cho cùng 1 vấn đề đã có 2 lớp xử lý rồi.

    return_full_checkpoint=False: CHỈ nhận phần MỚI fetch trong lần gọi này
    — vì mỗi lần gọi flow này là 1 process/run RIÊNG BIỆT (không phải gọi
    lại ngay trong cùng 1 process như trường hợp bắt RateLimitExhaustedError
    của weather_extractor) — load_mode='append' vào Postgres mà nhận cả
    checkpoint cũ sẽ NẠP TRÙNG dữ liệu đã load ở lần chạy trước.
    """
    logger = get_run_logger()

    result = weather_extractor.extract_all(
        checkpoint_path=WEATHER_CHECKPOINT_PATH,
        batch_size=batch_size,
        max_batches_per_run=max_batches_per_run,
        return_full_checkpoint=False,
    )

    is_complete = result.metadata.get("is_complete", False)
    batches_processed = result.metadata.get("batches_processed_this_run", 0)

    if result.df.empty:
        logger.info(f"Không fetch được zone mới nào trong lần chạy này (is_complete={is_complete}).")
        row_count = 0
    else:
        validated_df = validate_or_raise(result.df, result.table_name)

        save_to_raw_file(
            validated_df, source_name="weather", table_name="weather_daily", raw_dir="data/raw/weather"
        )

        row_count = load_dataframe(
            validated_df, source_name="weather", table_name="weather_daily", load_mode="append"
        )

    checkpoint_file = Path(WEATHER_CHECKPOINT_PATH)
    if is_complete:
        if checkpoint_file.exists():
            checkpoint_file.unlink()
        logger.info(f"Weather ingestion HOÀN TẤT toàn bộ zone — đã xoá checkpoint: {checkpoint_file}")
    else:
        logger.info(
            f"Lần chạy này: {batches_processed} batch, {row_count:,} dòng mới. "
            f"is_complete=False — checkpoint giữ lại, chạy flow này lần nữa để tiếp tục."
        )

    return {
        "rows_loaded": row_count,
        "is_complete": is_complete,
        "batches_processed": batches_processed,
        "zones_failed": result.metadata.get("zones_failed", []),
    }


@flow(name=FLOW_NAME, log_prints=False)
def weather_ingestion_flow(
    max_batches_per_run: int = DEFAULT_MAX_BATCHES_PER_RUN,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict:
    """
    Chạy 1 "lát" ingestion weather (tối đa max_batches_per_run batch). Gọi
    lại flow này (thủ công hoặc qua lịch) nhiều lần cho tới khi
    summary["is_complete"] == True.
    """
    logger = get_run_logger()
    summary = ingest_weather_batch(max_batches_per_run=max_batches_per_run, batch_size=batch_size)
    logger.info(f"Weather batch run summary: {summary}")
    return summary


if __name__ == "__main__":
    if "--serve" in sys.argv:
        # Chạy theo lịch qua chính Prefect (không cần cron của OS — tiện hơn
        # trên Windows, nơi cron không có sẵn). Mặc định mỗi giờ 1 lần;
        # đổi lại biểu thức cron này nếu muốn chạy dày/thưa hơn.
        weather_ingestion_flow.serve(name="weather-hourly", cron="0 * * * *")
    else:
        weather_ingestion_flow()