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
def _daily_entry(day_offset: int = 0):
    """1 phần tử 'daily' mẫu — dùng để dựng response batch nhiều zone."""
    return {
        "daily": {
            "time": ["2018-01-01", "2018-01-02"],
            "temperature_2m_max": [28.5, 27.0],
            "temperature_2m_min": [19.0, 18.5],
            "temperature_2m_mean": [23.5, 22.8],
            "precipitation_sum": [5.2, 0.0],
        }
    }


GOOD_DAILY_JSON = _daily_entry()  # response 1 zone (dict, không phải list)


def _batch_response_for(params: dict):
    """Mô phỏng Open-Meteo: N toạ độ trong request -> list N phần tử 'daily'."""
    n = len(params["latitude"].split(","))
    return [_daily_entry() for _ in range(n)]


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


class TestWeatherExtractorFetchBatch:
    def test_fetch_batch_single_zone_shape(self, mock_response_factory):
        """Batch chỉ 1 zone -> Open-Meteo trả dict (không phải list) -> vẫn phải parse đúng."""
        zones = pd.DataFrame({"zone_id": ["z1"], "centroid_lat": [-23.55], "centroid_lng": [-46.63]})

        class OkSession:
            def get(self, url, params=None, timeout=None):
                return mock_response_factory(200, GOOD_DAILY_JSON)

        df = weather_extractor._fetch_zone_batch(
            zones, "2018-01-01", "2018-01-02", session=OkSession()
        )
        expected_cols = {
            "date", "temp_max_c", "temp_min_c", "temp_avg_c",
            "precipitation_mm", "zone_id", "source",
        }
        assert set(df.columns) == expected_cols
        assert len(df) == 2
        assert (df["zone_id"] == "z1").all()

    def test_fetch_batch_multi_zone_maps_response_order_correctly(self, mock_response_factory):
        """3 zone trong 1 batch -> response list 3 phần tử, PHẢI gán đúng zone_id theo thứ tự."""
        zones = pd.DataFrame(
            {
                "zone_id": ["zA", "zB", "zC"],
                "centroid_lat": [-23.0, -22.0, -21.0],
                "centroid_lng": [-46.0, -45.0, -44.0],
            }
        )

        class OkSession:
            def get(self, url, params=None, timeout=None):
                assert params["latitude"] == "-23.0,-22.0,-21.0"
                assert params["longitude"] == "-46.0,-45.0,-44.0"
                return mock_response_factory(200, _batch_response_for(params))

        df = weather_extractor._fetch_zone_batch(
            zones, "2018-01-01", "2018-01-02", session=OkSession()
        )
        assert len(df) == 6  # 3 zone x 2 ngày
        assert set(df["zone_id"].unique()) == {"zA", "zB", "zC"}
        for z in ["zA", "zB", "zC"]:
            vals = df[df["zone_id"] == z].sort_values("date")["temp_max_c"].tolist()
            assert vals == [28.5, 27.0]

    def test_batch_response_length_mismatch_raises(self, mock_response_factory):
        """Response trả về SỐ PHẦN TỬ khác số zone đã gửi -> không thể ánh xạ zone_id, phải raise."""
        zones = pd.DataFrame(
            {"zone_id": ["zA", "zB"], "centroid_lat": [-23.0, -22.0], "centroid_lng": [-46.0, -45.0]}
        )

        class MismatchSession:
            def get(self, url, params=None, timeout=None):
                return mock_response_factory(200, [_daily_entry()])  # chỉ 1, thiếu 1

        with pytest.raises(ApiValidationError):
            weather_extractor._fetch_zone_batch(
                zones, "2018-01-01", "2018-01-02", session=MismatchSession()
            )

    def test_transient_network_error_recovers_on_retry(self, mock_response_factory):
        zones = pd.DataFrame({"zone_id": ["z1"], "centroid_lat": [-23.55], "centroid_lng": [-46.63]})

        class FlakySession:
            def __init__(self):
                self.calls = 0

            def get(self, url, params=None, timeout=None):
                self.calls += 1
                if self.calls == 1:
                    raise requests.exceptions.ConnectionError("reset")
                return mock_response_factory(200, GOOD_DAILY_JSON)

        session = FlakySession()
        df = weather_extractor._fetch_zone_batch(
            zones, "2018-01-01", "2018-01-02", session=session
        )
        assert len(df) == 2
        assert session.calls == 2

    def test_persistent_5xx_exhausts_retries_and_raises(self, mock_response_factory):
        zones = pd.DataFrame({"zone_id": ["z1"], "centroid_lat": [-23.55], "centroid_lng": [-46.63]})

        class AlwaysDownSession:
            def __init__(self):
                self.calls = 0

            def get(self, url, params=None, timeout=None):
                self.calls += 1
                return mock_response_factory(500, text="down")

        session = AlwaysDownSession()
        with pytest.raises(weather_extractor.BatchFetchError):
            weather_extractor._fetch_zone_batch(
                zones, "2018-01-01", "2018-01-02", session=session
            )
        assert session.calls == weather_extractor.MAX_RETRIES_PER_BATCH

    def test_non_retryable_400_fails_immediately_without_retry(self, mock_response_factory):
        zones = pd.DataFrame({"zone_id": ["z1"], "centroid_lat": [-999.0], "centroid_lng": [-46.63]})

        class BadParamsSession:
            def __init__(self):
                self.calls = 0

            def get(self, url, params=None, timeout=None):
                self.calls += 1
                return mock_response_factory(400, text="Bad Request")

        session = BadParamsSession()
        with pytest.raises(ApiValidationError):
            weather_extractor._fetch_zone_batch(
                zones, "2018-01-01", "2018-01-02", session=session
            )
        assert session.calls == 1  # không phí lượt retry cho lỗi tham số

    def test_429_uses_fixed_long_backoff_not_short_exponential(self, mock_response_factory, monkeypatch):
        """
        429 phải chờ RATE_LIMIT_BACKOFF_SECONDS (~65s) cố định — KHÔNG dùng
        exponential ngắn (2s/4s) vốn vô nghĩa với quota tính theo phút.
        """
        sleep_calls = []
        monkeypatch.setattr(weather_extractor.time, "sleep", lambda s: sleep_calls.append(s))

        zones = pd.DataFrame({"zone_id": ["z1"], "centroid_lat": [-23.55], "centroid_lng": [-46.63]})

        class RateLimitedSession:
            def get(self, url, params=None, timeout=None):
                return mock_response_factory(429, text="rate limited")

        with pytest.raises(weather_extractor.BatchFetchError):
            weather_extractor._fetch_zone_batch(
                zones, "2018-01-01", "2018-01-02", session=RateLimitedSession()
            )

        assert sleep_calls == [
            weather_extractor.RATE_LIMIT_BACKOFF_SECONDS,
            weather_extractor.RATE_LIMIT_BACKOFF_SECONDS,
        ]

    def test_5xx_uses_short_exponential_backoff(self, mock_response_factory, monkeypatch):
        """5xx (blip thoáng qua) vẫn dùng exponential ngắn 2s/4s, khác 429."""
        sleep_calls = []
        monkeypatch.setattr(weather_extractor.time, "sleep", lambda s: sleep_calls.append(s))

        zones = pd.DataFrame({"zone_id": ["z1"], "centroid_lat": [-23.55], "centroid_lng": [-46.63]})

        class DownSession:
            def get(self, url, params=None, timeout=None):
                return mock_response_factory(500, text="down")

        with pytest.raises(weather_extractor.BatchFetchError):
            weather_extractor._fetch_zone_batch(
                zones, "2018-01-01", "2018-01-02", session=DownSession()
            )

        assert sleep_calls == [2.0, 4.0]


class TestWeatherExtractorExtractAll:
    def test_extract_all_batches_reduce_total_requests(
        self, tmp_path, mock_response_factory
    ):
        """10 zone, batch_size=3 -> chỉ 4 request (10/3 làm tròn lên), không phải 10."""
        zones_df = pd.DataFrame(
            {
                "zone_id": [f"zone_{i:03d}" for i in range(10)],
                "centroid_lat": [-23.0 - i * 0.01 for i in range(10)],
                "centroid_lng": [-46.0] * 10,
            }
        )
        zone_path = tmp_path / "zones.csv"
        zones_df.to_csv(zone_path, index=False)

        class CountingSession:
            def __init__(self):
                self.calls = 0

            def get(self, url, params=None, timeout=None):
                self.calls += 1
                return mock_response_factory(200, _batch_response_for(params))

        session = CountingSession()
        result = weather_extractor.extract_all(
            zone_centroids_path=zone_path, session=session, batch_size=3
        )
        assert session.calls == 4  # ceil(10/3)
        assert result.metadata["zones_succeeded"] == 10
        assert result.df["zone_id"].nunique() == 10

    def test_extract_all_partial_batch_failure_does_not_block_others(
        self, tmp_path, mock_response_factory
    ):
        zones_df = pd.DataFrame(
            {
                "zone_id": [f"zone_{i:03d}" for i in range(9)],
                "centroid_lat": [-23.0 - i * 0.01 for i in range(9)],
                "centroid_lng": [-46.0] * 9,
            }
        )
        zone_path = tmp_path / "zones.csv"
        zones_df.to_csv(zone_path, index=False)

        class MiddleBatchDownSession:
            def get(self, url, params=None, timeout=None):
                lats = params["latitude"].split(",")
                first_idx = round((float(lats[0]) + 23.0) / -0.01)
                if first_idx == 3:  # batch giữa (zone 3,4,5) lỗi vĩnh viễn
                    return mock_response_factory(500, text="down")
                return mock_response_factory(200, _batch_response_for(params))

        result = weather_extractor.extract_all(
            zone_centroids_path=zone_path, session=MiddleBatchDownSession(), batch_size=3
        )
        assert result.metadata["zones_requested"] == 9
        assert result.metadata["zones_succeeded"] == 6
        assert set(result.metadata["zones_failed"]) == {"zone_003", "zone_004", "zone_005"}

    def test_extract_all_raises_when_all_batches_fail(
        self, tmp_path, sample_zone_centroids_df, mock_response_factory
    ):
        zone_path = tmp_path / "zone_centroids.csv"
        sample_zone_centroids_df.to_csv(zone_path, index=False)

        class AllDownSession:
            def get(self, url, params=None, timeout=None):
                return mock_response_factory(400, text="always bad")  # non-retryable -> ApiValidationError propagates

        # 400 không retry và raise ngay ApiValidationError -> extract_all bắt và coi là batch fail,
        # không phải RuntimeError toàn cục vì chỉ 1 batch (3 zone, batch_size mặc định 50)
        with pytest.raises(RuntimeError):
            weather_extractor.extract_all(zone_centroids_path=zone_path, session=AllDownSession())

    def test_consecutive_rate_limited_batches_abort_early_with_partial_results(
        self, tmp_path, mock_response_factory
    ):
        """
        Mô phỏng đúng sự cố thật: 1 vài batch đầu OK, sau đó TOÀN BỘ bị 429
        (hết hạn mức API toàn cục) — phải dừng SỚM, không lặp hết mọi batch.
        """
        zones_df = pd.DataFrame(
            {
                "zone_id": [f"zone_{i:03d}" for i in range(30)],
                "centroid_lat": [-23.0 - i * 0.01 for i in range(30)],
                "centroid_lng": [-46.0] * 30,
            }
        )
        zone_path = tmp_path / "zones.csv"
        zones_df.to_csv(zone_path, index=False)

        class RateLimitedSession:
            def __init__(self):
                self.calls = 0

            def get(self, url, params=None, timeout=None):
                self.calls += 1
                lats = params["latitude"].split(",")
                first_idx = round((float(lats[0]) + 23.0) / -0.01)
                if first_idx >= 6:  # từ batch thứ 3 trở đi (batch_size=3) đều bị rate limit
                    return mock_response_factory(429, text="rate limited")
                return mock_response_factory(200, _batch_response_for(params))

        session = RateLimitedSession()
        with pytest.raises(weather_extractor.RateLimitExhaustedError) as exc_info:
            weather_extractor.extract_all(
                zone_centroids_path=zone_path, session=session, batch_size=3
            )

        # Dừng sớm: KHÔNG được gọi API cho toàn bộ 10 batch x 3 retry (30 lần)
        assert session.calls < 30
        assert exc_info.value.partial_df["zone_id"].nunique() == 6  # 2 batch x 3 zone thành công

    def test_checkpoint_resume_skips_already_fetched_zones(
        self, tmp_path, mock_response_factory
    ):
        """Chạy lần 2 với checkpoint từ lần 1 KHÔNG được gọi lại zone đã thành công."""
        zones_df = pd.DataFrame(
            {
                "zone_id": [f"zone_{i:03d}" for i in range(6)],
                "centroid_lat": [-23.0 - i * 0.01 for i in range(6)],
                "centroid_lng": [-46.0] * 6,
            }
        )
        zone_path = tmp_path / "zones.csv"
        zones_df.to_csv(zone_path, index=False)
        checkpoint_path = tmp_path / "checkpoint.csv"

        class PartialFailSession:
            def __init__(self, fail_after):
                self.fail_after = fail_after
                self.requested_indices = []

            def get(self, url, params=None, timeout=None):
                lats = params["latitude"].split(",")
                first_idx = round((float(lats[0]) + 23.0) / -0.01)
                self.requested_indices.append(first_idx)
                if first_idx >= self.fail_after:
                    return mock_response_factory(429, text="rate limited")
                return mock_response_factory(200, _batch_response_for(params))

        session1 = PartialFailSession(fail_after=3)
        with pytest.raises(weather_extractor.RateLimitExhaustedError):
            weather_extractor.extract_all(
                zone_centroids_path=zone_path,
                session=session1,
                checkpoint_path=checkpoint_path,
                batch_size=3,
                consecutive_rate_limit_abort_threshold=1,
            )

        class AlwaysOkSession:
            def __init__(self):
                self.requested_indices = []

            def get(self, url, params=None, timeout=None):
                lats = params["latitude"].split(",")
                self.requested_indices.append(round((float(lats[0]) + 23.0) / -0.01))
                return mock_response_factory(200, _batch_response_for(params))

        session2 = AlwaysOkSession()
        result = weather_extractor.extract_all(
            zone_centroids_path=zone_path,
            session=session2,
            checkpoint_path=checkpoint_path,
            batch_size=3,
        )

        # 3 zone đầu ĐÃ thành công ở lần 1 -> lần 2 KHÔNG được gọi lại
        assert 0 not in session2.requested_indices
        assert result.df["zone_id"].nunique() == 6  # đủ cả 6 zone sau khi resume

    def test_max_batches_per_run_stops_gracefully_not_an_error(
        self, tmp_path, mock_response_factory
    ):
        """
        max_batches_per_run cho phép dừng CÓ CHỦ ĐÍCH sau N batch — KHÔNG
        phải lỗi, khác hẳn RateLimitExhaustedError.
        """
        zones_df = pd.DataFrame(
            {
                "zone_id": [f"zone_{i:03d}" for i in range(9)],
                "centroid_lat": [-23.0 - i * 0.01 for i in range(9)],
                "centroid_lng": [-46.0] * 9,
            }
        )
        zone_path = tmp_path / "zones.csv"
        zones_df.to_csv(zone_path, index=False)
        checkpoint_path = tmp_path / "checkpoint.csv"

        class AlwaysOkSession:
            def get(self, url, params=None, timeout=None):
                return mock_response_factory(200, _batch_response_for(params))

        result = weather_extractor.extract_all(
            zone_centroids_path=zone_path,
            session=AlwaysOkSession(),
            checkpoint_path=checkpoint_path,
            batch_size=3,
            max_batches_per_run=2,  # 9 zone / batch_size=3 = 3 batch total, chỉ làm 2
        )

        assert result.metadata["batches_processed_this_run"] == 2
        assert result.metadata["is_complete"] is False
        assert result.df["zone_id"].nunique() == 6  # 2 batch x 3 zone

        # Checkpoint phải đã lưu đúng 6 zone, sẵn sàng cho lần chạy sau resume nốt 3 zone còn lại
        checkpoint_df = pd.read_csv(checkpoint_path)
        assert checkpoint_df["zone_id"].nunique() == 6

    def test_return_full_checkpoint_false_only_returns_this_run_new_rows(
        self, tmp_path, mock_response_factory
    ):
        """
        return_full_checkpoint=False (dùng cho mô hình chạy theo lịch, mỗi
        lần 1 process riêng) -> CHỈ trả về phần MỚI fetch trong lần gọi này,
        tránh loader load_mode='append' bị nạp trùng dữ liệu đã load trước đó.
        """
        zones_df = pd.DataFrame(
            {
                "zone_id": [f"zone_{i:03d}" for i in range(6)],
                "centroid_lat": [-23.0 - i * 0.01 for i in range(6)],
                "centroid_lng": [-46.0] * 6,
            }
        )
        zone_path = tmp_path / "zones.csv"
        zones_df.to_csv(zone_path, index=False)
        checkpoint_path = tmp_path / "checkpoint.csv"

        class AlwaysOkSession:
            def get(self, url, params=None, timeout=None):
                return mock_response_factory(200, _batch_response_for(params))

        # Lần chạy 1 (giả lập 1 process riêng, vd chạy theo lịch): fetch 3 zone đầu
        result1 = weather_extractor.extract_all(
            zone_centroids_path=zone_path,
            session=AlwaysOkSession(),
            checkpoint_path=checkpoint_path,
            batch_size=3,
            max_batches_per_run=1,
            return_full_checkpoint=False,
        )
        assert result1.df["zone_id"].nunique() == 3  # CHỈ 3 zone mới fetch lần này

        # Lần chạy 2 (process khác, cùng checkpoint): fetch 3 zone còn lại
        result2 = weather_extractor.extract_all(
            zone_centroids_path=zone_path,
            session=AlwaysOkSession(),
            checkpoint_path=checkpoint_path,
            batch_size=3,
            max_batches_per_run=1,
            return_full_checkpoint=False,
        )
        # Kết quả lần 2 CHỈ chứa 3 zone MỚI của lần này, KHÔNG lẫn 3 zone đã trả ở lần 1
        assert result2.df["zone_id"].nunique() == 3
        assert set(result1.df["zone_id"]).isdisjoint(set(result2.df["zone_id"]))

    def test_already_complete_returns_clean_empty_result_not_error(
        self, tmp_path, mock_response_factory
    ):
        """Gọi lại khi checkpoint đã có ĐỦ mọi zone -> thành công, KHÔNG raise gì cả."""
        zones_df = pd.DataFrame(
            {"zone_id": ["z1", "z2"], "centroid_lat": [-23.0, -23.1], "centroid_lng": [-46.0, -46.1]}
        )
        zone_path = tmp_path / "zones.csv"
        zones_df.to_csv(zone_path, index=False)
        checkpoint_path = tmp_path / "checkpoint.csv"

        class AlwaysOkSession:
            def get(self, url, params=None, timeout=None):
                return mock_response_factory(200, _batch_response_for(params))

        weather_extractor.extract_all(
            zone_centroids_path=zone_path, session=AlwaysOkSession(), checkpoint_path=checkpoint_path
        )
        # Gọi lại lần 2 -> không còn zone nào để fetch
        result = weather_extractor.extract_all(
            zone_centroids_path=zone_path, session=AlwaysOkSession(), checkpoint_path=checkpoint_path
        )
        assert result.metadata["is_complete"] is True
        assert result.metadata["batches_processed_this_run"] == 0