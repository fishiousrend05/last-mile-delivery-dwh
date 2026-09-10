"""
ingestion/tests/test_extractors.py — Test olist_extractor.py và
weather_extractor.py. Không gọi API/đọc file thật ngoài tmp_path do pytest
tự dọn sau mỗi test.
"""

from __future__ import annotations

import pandas as pd
import pytest
import requests

from ingestion.extractors import olist_extractor, weather_extractor
from ingestion.extractors.base import ExtractionResult
from ingestion.validators.api_validator import ApiValidationError
from ingestion.validators.file_validator import FileValidationError


# ---------------------------------------------------------------------------
# olist_extractor.py
# ---------------------------------------------------------------------------
class TestOlistExtractor:
    def test_extract_table_happy_path(self, tmp_olist_dir):
        result = olist_extractor.extract_table("olist_orders", source_dir=tmp_olist_dir)
        assert isinstance(result, ExtractionResult)
        assert len(result.df) == 2
        assert result.metadata["table_name"] == "olist_orders"
        assert result.metadata["row_count_raw"] == 2
        assert "extracted_at" in result.metadata

    def test_extract_table_unknown_name_raises_keyerror(self, tmp_olist_dir):
        with pytest.raises(KeyError):
            olist_extractor.extract_table("khong_ton_tai", source_dir=tmp_olist_dir)

    def test_extract_table_missing_file_raises(self, tmp_olist_dir):
        with pytest.raises(FileValidationError):
            olist_extractor.extract_table("olist_sellers", source_dir=tmp_olist_dir)

    def test_extract_all_skips_missing_files_without_failing_batch(self, tmp_olist_dir):
        results = olist_extractor.extract_all(source_dir=tmp_olist_dir)
        table_names = [r.table_name for r in results]
        assert "olist_orders" in table_names
        assert "olist_customers" in table_names
        assert "olist_sellers" not in table_names  # file không tồn tại trong fixture
        assert len(results) < len(olist_extractor.OLIST_FILE_MAP)

    def test_extractor_does_not_coerce_dtypes(self, tmp_olist_dir):
        """Extractor phải trả về dữ liệu THÔ — không tự ép kiểu datetime/int."""
        result = olist_extractor.extract_table("olist_orders", source_dir=tmp_olist_dir)
        assert not pd.api.types.is_datetime64_any_dtype(result.df["order_status"])


# ---------------------------------------------------------------------------
# weather_extractor.py
# ---------------------------------------------------------------------------
GOOD_DAILY_JSON = {
    "daily": {
        "time": ["2018-01-01", "2018-01-02"],
        "temperature_2m_max": [28.5, 27.0],
        "temperature_2m_min": [19.0, 18.5],
        "temperature_2m_mean": [23.5, 22.8],
        "precipitation_sum": [5.2, 0.0],
    }
}


class TestWeatherExtractorZoneCentroids:
    def test_load_zone_centroids_happy_path(self, tmp_path, sample_zone_centroids_df):
        path = tmp_path / "zone_centroids.csv"
        sample_zone_centroids_df.to_csv(path, index=False)
        zones = weather_extractor.load_zone_centroids(path)
        assert len(zones) == 3

    def test_load_zone_centroids_missing_required_columns_raises(self, tmp_path):
        path = tmp_path / "bad_zones.csv"
        pd.DataFrame({"zone_id": ["z1"]}).to_csv(path, index=False)
        with pytest.raises(ValueError):
            weather_extractor.load_zone_centroids(path)

    def test_load_zone_centroids_missing_file_raises(self, tmp_path):
        with pytest.raises(FileValidationError):
            weather_extractor.load_zone_centroids(tmp_path / "nope.csv")


class TestWeatherExtractorFetchZone:
    def test_fetch_zone_weather_single_zone_shape(self, mock_response_factory):
        class OkSession:
            def get(self, url, params=None, timeout=None):
                return mock_response_factory(200, GOOD_DAILY_JSON)

        df = weather_extractor._fetch_zone_weather(
            "z1", -23.55, -46.63, "2018-01-01", "2018-01-02", session=OkSession()
        )
        expected_cols = {
            "date", "temp_max_c", "temp_min_c", "temp_avg_c",
            "precipitation_mm", "zone_id", "source",
        }
        assert set(df.columns) == expected_cols
        assert len(df) == 2
        assert (df["source"] == "open-meteo").all()

    def test_transient_network_error_recovers_on_retry(self, mock_response_factory):
        class FlakySession:
            def __init__(self):
                self.calls = 0

            def get(self, url, params=None, timeout=None):
                self.calls += 1
                if self.calls == 1:
                    raise requests.exceptions.ConnectionError("reset")
                return mock_response_factory(200, GOOD_DAILY_JSON)

        session = FlakySession()
        df = weather_extractor._fetch_zone_weather(
            "z1", -23.55, -46.63, "2018-01-01", "2018-01-02", session=session
        )
        assert len(df) == 2
        assert session.calls == 2

    def test_persistent_5xx_exhausts_retries_and_raises(self, mock_response_factory):
        class AlwaysDownSession:
            def __init__(self):
                self.calls = 0

            def get(self, url, params=None, timeout=None):
                self.calls += 1
                return mock_response_factory(500, text="down")

        session = AlwaysDownSession()
        with pytest.raises(weather_extractor.ZoneFetchError):
            weather_extractor._fetch_zone_weather(
                "z1", -23.55, -46.63, "2018-01-01", "2018-01-02", session=session
            )
        assert session.calls == weather_extractor.MAX_RETRIES_PER_ZONE

    def test_non_retryable_400_fails_immediately_without_retry(self, mock_response_factory):
        class BadParamsSession:
            def __init__(self):
                self.calls = 0

            def get(self, url, params=None, timeout=None):
                self.calls += 1
                return mock_response_factory(400, text="Bad Request")

        session = BadParamsSession()
        with pytest.raises(ApiValidationError):
            weather_extractor._fetch_zone_weather(
                "z1", -999, -46.63, "2018-01-01", "2018-01-02", session=session
            )
        assert session.calls == 1  # không phí lượt retry cho lỗi tham số


class TestWeatherExtractorExtractAll:
    def test_extract_all_partial_zone_failure_does_not_block_batch(
        self, tmp_path, sample_zone_centroids_df, mock_response_factory
    ):
        zone_path = tmp_path / "zone_centroids.csv"
        sample_zone_centroids_df.to_csv(zone_path, index=False)

        class MixedSession:
            def get(self, url, params=None, timeout=None):
                if params["latitude"] == -22.90:  # zone giữa bị lỗi vĩnh viễn
                    return mock_response_factory(500, text="down")
                return mock_response_factory(200, GOOD_DAILY_JSON)

        result = weather_extractor.extract_all(
            zone_centroids_path=zone_path, session=MixedSession()
        )
        assert result.metadata["zones_requested"] == 3
        assert result.metadata["zones_succeeded"] == 2
        assert result.metadata["zones_failed"] == ["852a1077fffffff"]
        assert len(result.df) == 4  # 2 zone thành công x 2 ngày

    def test_extract_all_raises_when_all_zones_fail(
        self, tmp_path, sample_zone_centroids_df, mock_response_factory
    ):
        zone_path = tmp_path / "zone_centroids.csv"
        sample_zone_centroids_df.to_csv(zone_path, index=False)

        class AllDownSession:
            def get(self, url, params=None, timeout=None):
                return mock_response_factory(500, text="down")

        with pytest.raises(RuntimeError):
            weather_extractor.extract_all(zone_centroids_path=zone_path, session=AllDownSession())