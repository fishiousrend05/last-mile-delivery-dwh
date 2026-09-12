"""
extractors/weather_extractor.py — Gọi Open-Meteo Historical Weather API theo
BATCH/REQUEST: nhiều zone (BATCH_SIZE) trong 1 request duy nhất, dùng comma-
separated latitude/longitude mà Open-Meteo hỗ trợ (multi-location request).

THIẾT KẾ LẠI (v2) — trước đây là "1 request / zone" (~6800 request), gây
chạm giới hạn hourly của free tier giữa chừng (Open-Meteo trả 429 "try again
in the next hour" hàng loạt). Batch 50 zone/request giảm còn ~140 request
cho toàn bộ zone_centroids.csv — giảm tải đúng gốc rễ, không chỉ né tránh.

Input:  data/raw/synthetic/zone_centroids.csv (do scripts/train_zone_clustering.py
        sinh ra) — cần cột zone_id, centroid_lat, centroid_lng.
Output: ExtractionResult với DataFrame có cột date/zone_id/temp_max_c/
        temp_min_c/temp_avg_c/precipitation_mm/source — khớp đúng schema
        weather_daily_schema trong ingestion/schemas/weather_schema.py.

Hợp đồng Extractor (giống olist_extractor.py, dùng chung ExtractionResult
từ extractors/base.py):
    Extractor (module này)           -> pd.DataFrame THÔ + metadata
    Validator (schema_validator.py)  -> pd.DataFrame SẠCH
    Loader                            -> ghi vào Postgres

Module này gọi api_validator.validate_response() (vòng ngoài) TRƯỚC khi parse
JSON — vẫn là việc của Extractor. KHÔNG tự ép kiểu/lọc range ở đây — đó là
việc của schema_validator.py ở bước sau.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
import yaml

from ingestion.extractors.base import ExtractionResult
from ingestion.utils.logger import get_logger
from ingestion.validators.api_validator import ApiValidationError, validate_response
from ingestion.validators.file_validator import validate_file

logger = get_logger(__name__)

_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "ingestion_config.yaml"
DEFAULT_ZONE_CENTROIDS_PATH = Path("data/raw/synthetic/zone_centroids.csv")

OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
DAILY_VARS = "temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum"
TIMEZONE = "America/Sao_Paulo"

# Số zone gộp vào 1 request — Open-Meteo hỗ trợ multi-location qua
# latitude/longitude dạng "lat1,lat2,..."/"lng1,lng2,...", trả về JSON array
# ĐÚNG THEO THỨ TỰ đã gửi.
#
# QUAN TRỌNG (phát hiện qua manual test thật): giới hạn của Open-Meteo có vẻ
# tính theo "chi phí" = số zone x số ngày trong request, KHÔNG PHẢI theo số
# lần gọi HTTP thuần tuý. Batch 50 zone x ~776 ngày (toàn bộ date range dự
# án) đã chạm ngay "Minutely API request limit" ở request thứ 2 — trong khi
# smoke test 3 zone x 7 ngày chạy trót lọt. => 10 là giá trị AN TOÀN HƠN,
# nhưng đây là số THỰC NGHIỆM, có thể cần tinh chỉnh thêm tuỳ hạn mức thật
# tại thời điểm chạy (Open-Meteo không công bố công thức tính cost rõ ràng).
DEFAULT_BATCH_SIZE = 10

# --- Self-throttle CHỦ ĐỘNG (phòng hơn chống) ---
# Quan sát thực nghiệm từ log E2E thật (batch_size=10, full date range):
# ĐÚNG 3 request thành công liên tiếp, rồi bị 429, đợi ~65-76s, rồi lại đúng
# 3 request thành công — 1 cửa sổ trượt rõ ràng. Thay vì đợi API từ chối rồi
# mới biết (reactive), tự đếm số request đã gửi trong WINDOW_SECONDS giây
# gần nhất và CHỦ ĐỘNG chờ trước khi vượt ngưỡng — tránh lãng phí round-trip
# tới API chỉ để nhận về 429 mà lẽ ra có thể đoán trước được.
#
# Đây vẫn là số THỰC NGHIỆM (Open-Meteo không công bố công thức) — nếu vẫn
# bị 429 dù đã self-throttle, RateLimitExhaustedError bên dưới vẫn là lưới
# an toàn cuối cùng.
REQUESTS_PER_WINDOW = 3
WINDOW_SECONDS = 70.0  # hơi cao hơn 65s quan sát được, để có biên an toàn

# --- Cấu hình retry CỤC BỘ cho 1 BATCH (Circuit Breaker cấp batch) ---
# Triết lý: Prefect task-level retry chỉ nên dùng cho lỗi DIỆN RỘNG (DB chết,
# cả API Open-Meteo bảo trì) — vì nó chạy lại TOÀN BỘ task từ đầu, tốn công +
# hạn mức API kéo lại những batch ĐÃ thành công trước đó. Lỗi CỤC BỘ (rớt
# mạng thoáng qua ở 1 batch, rate limit tạm thời) phải được Extractor tự
# đứng lên xử lý tại chỗ, không để lan ra ngoài làm hỏng cả lần chạy dở.
MAX_RETRIES_PER_BATCH = 3

# Backoff cho lỗi 5xx (server quá tải CHỚP NHOÁNG) — exponential ngắn vẫn hợp
# lý vì bản chất là blip thoáng qua.
SERVER_ERROR_BACKOFF_BASE_SECONDS = 2.0  # 2s, 4s, 8s...

# Backoff RIÊNG cho 429 (rate limit) — dùng số CỐ ĐỊNH, không phải exponential
# ngắn, vì retry sau 2s/4s/8s KHÔNG có ý nghĩa gì với 1 cửa sổ quota tính theo
# PHÚT (Open-Meteo trả "try again in one minute" — nghĩa là chờ 2s/4s/8s rồi
# thử lại chắc chắn vẫn nằm trong đúng cửa sổ bị chặn). 65s (hơi hơn 1 phút)
# để chắc chắn đã qua cửa sổ, thay vì đoán mò với backoff kiểu 5xx.
RATE_LIMIT_BACKOFF_SECONDS = 65.0

# Status code coi là TẠM THỜI, đáng để retry (rate limit, server quá tải chớp
# nhoáng) — khác với 4xx (sai tham số) vốn retry lại cũng ra y hệt kết quả.
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class BatchFetchError(Exception):
    """
    Raise khi 1 batch đã retry hết MAX_RETRIES_PER_BATCH lần vẫn thất bại vì
    lý do TẠM THỜI (mạng/rate limit) — lỗi CỤC BỘ của riêng batch đó.
    extract_all() bắt lỗi này, ghi các zone trong batch vào failed_zones, và
    tiếp tục batch kế tiếp — KHÔNG để lỗi này văng lên tới Prefect để nó
    retry lại cả task.

    status_code cho extract_all() biết ĐÂY LÀ LỖI GÌ — quan trọng để phân
    biệt 429 (rate limit) với 5xx, vì 429 ở nhiều batch LIÊN TIẾP là dấu
    hiệu HẾT HẠN MỨC TOÀN CỤC (không phải lỗi cục bộ từng batch) — xem
    RateLimitExhaustedError.
    """

    def __init__(self, message: str, zone_ids: list[str], status_code: int | None = None):
        super().__init__(message)
        self.zone_ids = zone_ids
        self.status_code = status_code


class RateLimitExhaustedError(Exception):
    """
    Raise khi phát hiện NHIỀU batch LIÊN TIẾP đều fail vì 429 — dấu hiệu rõ
    ràng của việc hạn mức API đã hết cho CẢ TÀI KHOẢN/IP (vd Open-Meteo trả
    "try again in the next hour"), KHÔNG phải lỗi cục bộ của từng batch.

    Khác với BatchFetchError (retry tại chỗ là hợp lý), trường hợp này retry
    từng batch là VÔ NGHĨA — batch tiếp theo chắc chắn cũng bị 429 ngay lập
    tức. Dừng SỚM thay vì lặp hết toàn bộ batch còn lại.

    partial_df/metadata mang theo kết quả ĐÃ fetch thành công trước khi phát
    hiện — để caller vẫn lưu được phần đã có, không mất trắng tiến độ.
    """

    def __init__(self, message: str, partial_df: pd.DataFrame, metadata: dict):
        super().__init__(message)
        self.partial_df = partial_df
        self.metadata = metadata


_DEFAULT_WEATHER_CONFIG = {
    "start": "2016-09-01",
    "end": "2018-10-31",
    "rate_limit_per_minute": 60,
}


def _load_weather_config() -> dict:
    """Đọc date_range + rate_limit_per_minute của nguồn weather từ ingestion_config.yaml."""
    if not _CONFIG_PATH.exists():
        logger.warning(f"Config not found at {_CONFIG_PATH}, using default weather config")
        return dict(_DEFAULT_WEATHER_CONFIG)

    with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}
    weather_cfg = config.get("sources", {}).get("weather", {})
    date_range = weather_cfg.get("date_range", {})
    return {
        "start": date_range.get("start", _DEFAULT_WEATHER_CONFIG["start"]),
        "end": date_range.get("end", _DEFAULT_WEATHER_CONFIG["end"]),
        "rate_limit_per_minute": weather_cfg.get(
            "rate_limit_per_minute", _DEFAULT_WEATHER_CONFIG["rate_limit_per_minute"]
        ),
    }


def load_zone_centroids(path: str | Path = DEFAULT_ZONE_CENTROIDS_PATH) -> pd.DataFrame:
    """
    Đọc bảng zone centroid do scripts/train_zone_clustering.py sinh ra.

    Đây LÀ input của module này, KHÔNG phải output — nếu file thiếu, extractor
    chỉ báo lỗi rõ ràng (gợi ý chạy lại script clustering), KHÔNG tự chạy lại
    clustering thay người dùng.
    """
    validated_path = validate_file(path, expected_extension=".csv")
    df = pd.read_csv(validated_path)

    required_cols = {"zone_id", "centroid_lat", "centroid_lng"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(
            f"'{validated_path}' missing required column(s): {missing}. "
            f"Re-run scripts/train_zone_clustering.py to regenerate it."
        )

    logger.info(f"Loaded {len(df):,} zone centroid(s) from {validated_path}")
    return df


def _parse_daily_response(zone_id: str, entry: dict) -> pd.DataFrame:
    """Reshape 1 phần tử 'daily' của Open-Meteo thành DataFrame thô 1 zone."""
    if "daily" not in entry:
        raise ApiValidationError(f"Missing 'daily' key for zone '{zone_id}' in batch response")
    daily = entry["daily"]
    df_zone = pd.DataFrame(
        {
            "date": daily["time"],
            "temp_max_c": daily["temperature_2m_max"],
            "temp_min_c": daily["temperature_2m_min"],
            "temp_avg_c": daily["temperature_2m_mean"],
            "precipitation_mm": daily["precipitation_sum"],
        }
    )
    df_zone["zone_id"] = zone_id
    df_zone["source"] = "open-meteo"
    return df_zone


def _parse_batch_response(zone_ids: list[str], body) -> pd.DataFrame:
    """
    Open-Meteo trả về JSON ARRAY khi request có nhiều toạ độ, nhưng trả về
    1 dict (không phải list) nếu request chỉ có ĐÚNG 1 toạ độ (vd batch cuối
    chỉ còn 1 zone) — chuẩn hoá cả 2 trường hợp về list trước khi xử lý.

    Thứ tự phần tử trong response body PHẢI khớp thứ tự zone_ids đã gửi
    (Open-Meteo đảm bảo giữ nguyên thứ tự multi-location request).
    """
    entries = [body] if isinstance(body, dict) else body

    if len(entries) != len(zone_ids):
        raise ApiValidationError(
            f"Batch response length mismatch: gửi {len(zone_ids)} zone, "
            f"nhận {len(entries)} phần tử — không thể ánh xạ đúng zone_id"
        )

    frames = [_parse_daily_response(zid, entry) for zid, entry in zip(zone_ids, entries)]
    return pd.concat(frames, ignore_index=True)


def _fetch_zone_batch(
    zones: pd.DataFrame,
    start_date: str,
    end_date: str,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """
    Gọi Open-Meteo cho NHIỀU zone (1 batch) trong 1 request duy nhất, TOÀN BỘ
    date range. Tự retry TẠI CHỖ khi gặp lỗi TẠM THỜI — timeout/connection
    error, hoặc status code trong RETRYABLE_STATUS_CODES (429/5xx). Backoff
    tăng dần (2s, 4s, 8s).

    Lỗi KHÔNG tạm thời (4xx, JSON hỏng, length mismatch) raise NGAY LẬP TỨC,
    không phí lượt retry.

    `session` cho phép truyền vào 1 requests.Session dùng chung hoặc object
    giả lập khi unit test.

    Raises:
        BatchFetchError nếu hết MAX_RETRIES_PER_BATCH lần mà vẫn lỗi tạm thời.
        ApiValidationError NGAY LẬP TỨC nếu lỗi không tạm thời (không retry).
    """
    http_get = session.get if session is not None else requests.get
    zone_ids = zones["zone_id"].tolist()
    params = {
        "latitude": ",".join(str(v) for v in zones["centroid_lat"]),
        "longitude": ",".join(str(v) for v in zones["centroid_lng"]),
        "start_date": start_date,
        "end_date": end_date,
        "daily": DAILY_VARS,
        "timezone": TIMEZONE,
    }

    last_error: Exception | None = None
    last_status_code: int | None = None
    batch_label = f"{zone_ids[0]}..{zone_ids[-1]}" if len(zone_ids) > 1 else zone_ids[0]

    for attempt in range(1, MAX_RETRIES_PER_BATCH + 1):
        try:
            # timeout dài hơn per-zone trước đây vì response giờ lớn hơn
            # (nhiều zone gộp lại).
            response = http_get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=60)
        except requests.exceptions.RequestException as e:
            last_error = e
            last_status_code = None
            logger.warning(
                f"[batch {batch_label}] Network error on attempt {attempt}/{MAX_RETRIES_PER_BATCH}: {e}"
            )
        else:
            if response.status_code == 200:
                body = validate_response(response, source_name="open-meteo")
                return _parse_batch_response(zone_ids, body)

            if response.status_code in RETRYABLE_STATUS_CODES:
                last_error = ApiValidationError(
                    f"Retryable status {response.status_code}: {response.text[:200]}"
                )
                last_status_code = response.status_code
                logger.warning(
                    f"[batch {batch_label}] Retryable status {response.status_code} "
                    f"on attempt {attempt}/{MAX_RETRIES_PER_BATCH}"
                )
            else:
                # Lỗi KHÔNG nên retry (vd 400 sai tham số) -> raise NGAY.
                validate_response(response, source_name="open-meteo")

        if attempt < MAX_RETRIES_PER_BATCH:
            if last_status_code == 429:
                # Cửa sổ quota tính theo phút -> chờ CỐ ĐỊNH ~65s, không
                # phải exponential ngắn (2s/4s/8s vô nghĩa với loại lỗi này).
                backoff = RATE_LIMIT_BACKOFF_SECONDS
            else:
                backoff = SERVER_ERROR_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
            logger.info(f"[batch {batch_label}] Retrying in {backoff:.0f}s...")
            time.sleep(backoff)

    raise BatchFetchError(
        f"Batch [{batch_label}] ({len(zone_ids)} zone) failed after "
        f"{MAX_RETRIES_PER_BATCH} attempt(s): {last_error}",
        zone_ids=zone_ids,
        status_code=last_status_code,
    )


def extract_all(
    zone_centroids_path: str | Path = DEFAULT_ZONE_CENTROIDS_PATH,
    session: requests.Session | None = None,
    checkpoint_path: str | Path | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    consecutive_rate_limit_abort_threshold: int = 3,
    max_batches_per_run: int | None = None,
    return_full_checkpoint: bool = True,
) -> ExtractionResult:
    """
    Extract weather cho zone trong zone_centroids.csv, gộp BATCH_SIZE
    zone/request để giảm mạnh tổng số HTTP request (~6800 zone / 10 mỗi
    batch ≈ 686 request thay vì ~6800).

    TỰ THEO DÕI ngân sách quota theo cửa sổ trượt (self-throttle) — CHỦ ĐỘNG
    dừng trước khi gửi request thứ (REQUESTS_PER_WINDOW + 1) trong
    WINDOW_SECONDS giây gần nhất, thay vì đợi API trả 429 rồi mới biết.
    RATE_LIMIT_BACKOFF_SECONDS/RateLimitExhaustedError vẫn giữ lại làm lưới
    an toàn (phòng trường hợp ước tính cửa sổ sai hoặc hạn mức API đổi).

    max_batches_per_run: nếu truyền vào, DỪNG CÓ CHỦ ĐÍCH sau khi xử lý đúng
    N batch trong lần gọi NÀY (không phải lỗi — trả về ExtractionResult bình
    thường với metadata["is_complete"]=False). Dùng cho mô hình chạy theo
    lịch (Prefect deployment/cron): mỗi lần chạy xử lý 1 lượng nhỏ, dựa vào
    checkpoint_path để lần sau tự resume tiếp — KHÔNG cần 1 tiến trình chạy
    liên tục nhiều giờ.

    return_full_checkpoint: mặc định True (giữ hành vi cũ — trả về TOÀN BỘ
    nội dung checkpoint, cả từ lần chạy trước lẫn lần này — phù hợp khi
    RateLimitExhaustedError được bắt và gọi lại NGAY trong cùng 1 process).
    Đặt False khi mỗi lần gọi extract_all() là 1 PROCESS RIÊNG BIỆT theo
    lịch (weather_ingestion_flow.py) — lúc đó chỉ nên trả về phần MỚI fetch
    trong lần gọi này, để loader dùng load_mode='append' không bị NẠP TRÙNG
    các zone đã load vào Postgres ở lần chạy trước.
    """
    zones_df = load_zone_centroids(zone_centroids_path)
    config = _load_weather_config()
    sleep_seconds = 60.0 / config["rate_limit_per_minute"]

    checkpoint_path = Path(checkpoint_path) if checkpoint_path else None
    already_fetched: set[str] = set()
    if checkpoint_path is not None and checkpoint_path.exists():
        existing = pd.read_csv(checkpoint_path, usecols=["zone_id"])
        already_fetched = set(existing["zone_id"].unique())
        logger.info(
            f"Checkpoint found at {checkpoint_path} — {len(already_fetched):,} zone(s) "
            f"already fetched, will be SKIPPED (resume mode)"
        )

    zones_to_fetch = zones_df[~zones_df["zone_id"].isin(already_fetched)].reset_index(drop=True)
    n_batches_total = -(-len(zones_to_fetch) // batch_size)  # ceil division

    if len(zones_to_fetch) == 0:
        # Không phải lỗi — nghĩa là lần chạy TRƯỚC đã fetch xong hết rồi.
        logger.info("Tất cả zone đã có trong checkpoint — không còn gì để fetch.")
        empty_df = pd.DataFrame(
            columns=["date", "temp_max_c", "temp_min_c", "temp_avg_c", "precipitation_mm", "zone_id", "source"]
        )
        return ExtractionResult(
            table_name="weather_daily",
            df=empty_df,
            metadata={
                "source_name": "weather",
                "table_name": "weather_daily",
                "zones_requested": len(zones_df),
                "zones_succeeded": len(already_fetched),
                "zones_failed": [],
                "row_count_raw": 0,
                "extracted_at": datetime.now(timezone.utc).isoformat(),
                "is_complete": True,
                "batches_processed_this_run": 0,
            },
        )

    n_batches_this_run = (
        min(max_batches_per_run, n_batches_total) if max_batches_per_run else n_batches_total
    )
    logger.info(
        f"Fetching weather for {len(zones_to_fetch):,}/{len(zones_df):,} zone(s) — "
        f"{n_batches_this_run}/{n_batches_total} batch(es) trong lần chạy này "
        f"(batch_size={batch_size}, date range {config['start']} -> {config['end']})"
        + (f", {len(already_fetched):,} zone(s) đã có sẵn từ checkpoint" if already_fetched else "")
    )

    all_frames: list[pd.DataFrame] = []
    failed_zones: list[str] = []
    consecutive_rate_limit_failures = 0
    recent_request_times: list[float] = []  # self-throttle: dấu thời gian các request gần nhất
    batches_processed = 0

    for start_idx in range(0, len(zones_to_fetch), batch_size):
        if max_batches_per_run is not None and batches_processed >= max_batches_per_run:
            logger.info(
                f"Đạt giới hạn max_batches_per_run={max_batches_per_run} — dừng có chủ đích, "
                f"phần còn lại để lần chạy sau (checkpoint đã lưu tiến độ)."
            )
            break

        # --- Self-throttle CHỦ ĐỘNG: dựa trên pattern thực nghiệm quan sát
        # được (REQUESTS_PER_WINDOW request thành công / WINDOW_SECONDS giây
        # rồi bị 429) — chủ động chờ TRƯỚC khi vượt ngưỡng, thay vì đợi API
        # từ chối rồi mới biết.
        now = time.monotonic()
        recent_request_times = [t for t in recent_request_times if now - t < WINDOW_SECONDS]
        if len(recent_request_times) >= REQUESTS_PER_WINDOW:
            wait_for = WINDOW_SECONDS - (now - recent_request_times[0]) + 0.5
            if wait_for > 0:
                logger.info(
                    f"Self-throttle: đã gửi {REQUESTS_PER_WINDOW} request trong "
                    f"{WINDOW_SECONDS}s gần nhất -> chủ động chờ {wait_for:.0f}s trước "
                    f"khi gửi tiếp (tránh bị 429)."
                )
                time.sleep(wait_for)

        batch = zones_to_fetch.iloc[start_idx : start_idx + batch_size]
        recent_request_times.append(time.monotonic())

        try:
            df_batch = _fetch_zone_batch(
                batch, start_date=config["start"], end_date=config["end"], session=session
            )
            all_frames.append(df_batch)
            batches_processed += 1
            consecutive_rate_limit_failures = 0  # reset ngay khi có 1 batch thành công

            if checkpoint_path is not None:
                write_header = not checkpoint_path.exists()
                checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
                df_batch.to_csv(checkpoint_path, mode="a", header=write_header, index=False)

        except (BatchFetchError, ApiValidationError, requests.RequestException) as e:
            batch_zone_ids = getattr(e, "zone_ids", batch["zone_id"].tolist())
            logger.error(f"Failed to fetch weather batch [{batch_zone_ids[0]}..{batch_zone_ids[-1]}]: {e}")
            failed_zones.extend(batch_zone_ids)
            batches_processed += 1

            status_code = getattr(e, "status_code", None)
            if status_code == 429:
                consecutive_rate_limit_failures += 1
            else:
                consecutive_rate_limit_failures = 0

            if consecutive_rate_limit_failures >= consecutive_rate_limit_abort_threshold:
                partial_df = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()
                metadata = {
                    "source_name": "weather",
                    "table_name": "weather_daily",
                    "zones_requested": len(zones_df),
                    "zones_succeeded": (
                        partial_df["zone_id"].nunique() if not partial_df.empty else 0
                    )
                    + len(already_fetched),
                    "zones_failed": failed_zones,
                    "row_count_raw": len(partial_df),
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                    "aborted_reason": "rate_limit_exhausted",
                }
                raise RateLimitExhaustedError(
                    f"{consecutive_rate_limit_failures} batch liên tiếp đều bị rate limit (429) — "
                    f"nghi ngờ hạn mức API đã hết TOÀN CỤC theo cách KHÁC với self-throttle đã ước "
                    f"tính (xem lại REQUESTS_PER_WINDOW/WINDOW_SECONDS). Đã fetch thành công "
                    f"{metadata['zones_succeeded']:,} zone. Đợi hạn mức API reset rồi chạy lại — "
                    "nếu có checkpoint_path, các zone đã thành công sẽ tự động được bỏ qua.",
                    partial_df=partial_df,
                    metadata=metadata,
                )

        time.sleep(sleep_seconds)  # tôn trọng rate_limit_per_minute config, tính theo BATCH

    if not all_frames and not already_fetched:
        # KHÔNG fetch được batch nào trong lần chạy này, VÀ cũng chưa từng
        # có zone nào từ trước (checkpoint rỗng) -> dấu hiệu API hỏng hoàn
        # toàn (sai URL, DNS, server down thật sự) chứ không phải "hết việc
        # để làm" — khác hẳn case len(zones_to_fetch)==0 đã xử lý riêng ở trên.
        raise RuntimeError("Weather extraction failed for ALL batches — check API/network status")

    is_complete = batches_processed >= n_batches_total

    if return_full_checkpoint and checkpoint_path is not None and checkpoint_path.exists():
        # Đọc lại TOÀN BỘ checkpoint (cả zone từ lần chạy trước lẫn vừa fetch
        # trong lần này) để trả về đúng 1 kết quả đầy đủ — CHỈ dùng khi gọi
        # lại NGAY trong cùng 1 process (vd retry sau RateLimitExhaustedError).
        df = pd.read_csv(checkpoint_path)
        df["date"] = pd.to_datetime(df["date"])
    elif all_frames:
        df = pd.concat(all_frames, ignore_index=True)
    else:
        df = pd.DataFrame(
            columns=["date", "temp_max_c", "temp_min_c", "temp_avg_c", "precipitation_mm", "zone_id", "source"]
        )

    if df.empty and not already_fetched and not return_full_checkpoint:
        # Lần chạy này không fetch được gì cả (vd batch đầu tiên đã lỗi) —
        # vẫn không phải lỗi nếu max_batches_per_run đang giới hạn số lần
        # thử, nhưng nếu 0 batch nào thành công thì nên cảnh báo rõ.
        logger.warning("Lần chạy này không fetch thành công zone nào.")

    metadata = {
        "source_name": "weather",
        "table_name": "weather_daily",
        "zones_requested": len(zones_df),
        "zones_succeeded": (
            df["zone_id"].nunique() if not df.empty else 0
        )
        + (len(already_fetched) if return_full_checkpoint else 0),
        "zones_failed": failed_zones,
        "row_count_raw": len(df),
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "is_complete": is_complete,
        "batches_processed_this_run": batches_processed,
    }

    logger.info(
        f"Weather extraction (lần chạy này) done — {batches_processed}/{n_batches_this_run} "
        f"batch xử lý, {len(df):,} dòng, is_complete={is_complete}"
    )
    return ExtractionResult(table_name="weather_daily", df=df, metadata=metadata)