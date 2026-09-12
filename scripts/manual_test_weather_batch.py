"""
Manual smoke test: gọi Open-Meteo thật cho 1 batch nhỏ.
Chạy: python -m scripts.manual_test_weather_batch
"""

from __future__ import annotations

import pandas as pd

from ingestion.extractors import weather_extractor


def main() -> None:
    # 3 thành phố lớn của Brazil — batch nhỏ để dễ soi kết quả
    zones = pd.DataFrame(
        {
            "zone_id": ["sao_paulo", "rio", "brasilia"],
            "centroid_lat": [-23.55, -22.91, -15.78],
            "centroid_lng": [-46.63, -43.17, -47.93],
        }
    )

    print(">>> Gọi Open-Meteo cho 3 zone, 1 request duy nhất...")
    df = weather_extractor._fetch_zone_batch(
        zones,
        start_date="2018-01-01",
        end_date="2018-01-07",   # 1 tuần cho nhẹ
        session=None,            # None -> dùng requests.get thật
    )

    print(f"\n>>> OK. Shape: {df.shape}")
    print(f">>> Columns: {list(df.columns)}")
    print(f">>> Zones: {sorted(df['zone_id'].unique())}")
    print("\n>>> 5 dòng đầu:")
    print(df.head())

    print("\n>>> Pivot temp_max_c theo zone để dễ soi thứ tự mapping:")
    print(
        df.pivot_table(
            index="date", columns="zone_id", values="temp_max_c"
        )
    )


if __name__ == "__main__":
    main()