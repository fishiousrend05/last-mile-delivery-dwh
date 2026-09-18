"""
scripts/train_zone_clustering.py — Sinh bảng zone theo H3 resolution 5, dùng
làm INPUT CHUNG cho các module downstream:
    - ingestion/extractors/weather_extractor.py (centroid lat/lng để gọi
      Open-Meteo theo batch/zone)
    - ingestion/generators/synthetic_generator.py (gán zone_id cho driver)
    - dbt seed cho dim_zone

Đây là SCRIPT ONE-OFF (không thuộc Prefect flow định kỳ) — chạy tay khi cần
rebuild lại danh sách zone. Logic dọn lại từ Dim_zone.ipynb (cell H3 chính
thức), bỏ toàn bộ phần thử nghiệm KMeans/so sánh weather (đã hoàn thành vai
trò làm bằng chứng cho quyết định chuyển từ KMeans sang H3 res5, không cần
chạy lại mỗi lần rebuild zone).

Input:  data/source/olist/olist_geolocation_dataset.csv
Output: dbt/seeds/zone_centroids.csv
        Cột: zone_id, centroid_lat, centroid_lng, point_count, dominant_state
        dbt/seeds/zip_zone_mapping.csv
        Cột: zip_code_prefix, zone_id

"point_count" tính trên TOÀN BỘ điểm gốc (chưa dedup) join lại theo zone —
không chỉ đếm toạ độ unique — để phản ánh đúng mật độ hoạt động thật của zone.
"dominant_state" (state xuất hiện nhiều nhất trong zone) chỉ mang tính tham
khảo, dùng để hậu kiểm xem 1 zone có bị tràn qua nhiều bang không — H3 res5
(~10km/cell) gần như không xảy ra trường hợp này, khác với KMeans k=20 trước
đó (đã phát hiện cụm bán kính ~955km trong quá trình đánh giá rủi ro).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from ingestion.utils.geo import RESOLUTION, h3_to_center, latlng_to_h3
from ingestion.utils.logger import get_logger
from ingestion.validators.file_validator import validate_file

logger = get_logger(__name__)

# Bounding box gần đúng của Brazil — loại nhiễu toạ độ nằm ngoài lãnh thổ,
# đã phát hiện lúc audit 9 file Olist ở Phase thiết kế.
BRAZIL_LAT_RANGE = (-35.0, 5.0)
BRAZIL_LNG_RANGE = (-75.0, -30.0)

DEFAULT_INPUT_PATH = Path("data/source/olist/olist_geolocation_dataset.csv")
DEFAULT_OUTPUT_PATH = Path("dbt/seeds/zone_centroids.csv")
DEFAULT_MAPPING_OUTPUT_PATH = Path("dbt/seeds/zip_zone_mapping.csv")  # MỚI


def load_geolocation(input_path: Path) -> pd.DataFrame:
    """Đọc geolocation CSV, qua file_validator (vòng ngoài) trước khi vào RAM."""
    validated_path = validate_file(input_path, expected_extension=".csv")
    logger.info(f"Reading geolocation data from {validated_path}")
    df = pd.read_csv(validated_path)
    logger.info(f"Loaded {len(df):,} raw geolocation rows")
    return df


def filter_brazil_bounds(df: pd.DataFrame) -> pd.DataFrame:
    """Loại các điểm ngoài bounding box Brazil — nhiễu toạ độ đã phát hiện lúc audit."""
    before = len(df)
    df_filtered = df[
        df["geolocation_lat"].between(*BRAZIL_LAT_RANGE)
        & df["geolocation_lng"].between(*BRAZIL_LNG_RANGE)
    ].copy()
    dropped = before - len(df_filtered)
    logger.info(f"Filtered to Brazil bounds: dropped {dropped:,} out-of-bounds row(s)")
    return df_filtered


def assign_zone_ids(df: pd.DataFrame, resolution: int = RESOLUTION) -> pd.DataFrame:
    """
    Gán zone_id (H3 index) cho từng điểm toạ độ gốc. Trả về df GỐC (chưa dedup)
    kèm cột zone_id — tách ra từ build_zone_table cũ để dùng chung cho cả
    centroid rollup lẫn zip -> zone mapping, tránh tính H3 hai lần.
    """
    df_unique = df.drop_duplicates(subset=["geolocation_lat", "geolocation_lng"]).copy()
    logger.info(f"{len(df_unique):,} unique coordinate pair(s) after dedup")

    logger.info(f"Mapping coordinates to H3 index (resolution={resolution})...")
    df_unique["zone_id"] = df_unique.apply(
        lambda row: latlng_to_h3(row["geolocation_lat"], row["geolocation_lng"], resolution),
        axis=1,
    )

    df_full = df.merge(
        df_unique[["geolocation_lat", "geolocation_lng", "zone_id"]],
        on=["geolocation_lat", "geolocation_lng"],
        how="inner",
    )
    return df_full


def build_zone_table(df_full: pd.DataFrame) -> pd.DataFrame:
    """Tổng hợp df_full (đã có zone_id) thành 1 dòng / zone: centroid, point_count, dominant_state."""
    zone_stats = (
        df_full.groupby("zone_id")
        .agg(
            point_count=("zone_id", "size"),
            dominant_state=("geolocation_state", lambda s: s.mode().iloc[0]),
        )
        .reset_index()
    )

    unique_zone_ids = df_full["zone_id"].unique()
    centroids = pd.DataFrame(
        [
            {"zone_id": zid, **dict(zip(["centroid_lat", "centroid_lng"], h3_to_center(zid)))}
            for zid in unique_zone_ids
        ]
    )

    zone_table = centroids.merge(zone_stats, on="zone_id", how="left")
    logger.info(f"Built {len(zone_table):,} H3 zone(s)")
    return zone_table


def build_zip_zone_mapping(df_full: pd.DataFrame) -> pd.DataFrame:
    """
    Map zip_code_prefix -> 1 zone_id đại diện (mode). Một zip hiếm khi trải
    >1 H3 cell (res5, ~10km/cell), nhưng vẫn có thể xảy ra ở vùng biên ->
    lấy zone_id xuất hiện nhiều nhất, cùng cách xử lý dominant_state đã dùng.
    """
    mapping = (
        df_full.groupby("geolocation_zip_code_prefix")["zone_id"]
        .agg(lambda s: s.mode().iloc[0])
        .reset_index()
        .rename(columns={"geolocation_zip_code_prefix": "zip_code_prefix"})
    )
    logger.info(f"Built zip -> zone mapping for {len(mapping):,} zip_code_prefix(es)")
    return mapping


def main(
    input_path: Path = DEFAULT_INPUT_PATH,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    mapping_output_path: Path = DEFAULT_MAPPING_OUTPUT_PATH,
) -> pd.DataFrame:
    df_geo = load_geolocation(input_path)
    df_geo = filter_brazil_bounds(df_geo)

    df_full = assign_zone_ids(df_geo)
    zone_table = build_zone_table(df_full)
    zip_mapping = build_zip_zone_mapping(df_full)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    zone_table.to_csv(output_path, index=False)
    logger.info(f"Saved zone centroid table to {output_path} ({len(zone_table):,} zone(s))")

    mapping_output_path.parent.mkdir(parents=True, exist_ok=True)
    zip_mapping.to_csv(mapping_output_path, index=False)
    logger.info(f"Saved zip->zone mapping to {mapping_output_path} ({len(zip_mapping):,} row(s))")

    return zone_table


if __name__ == "__main__":
    main()