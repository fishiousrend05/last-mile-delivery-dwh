"""
ingestion/tests/test_generators.py

Unit tests cho ingestion/generators/synthetic_generator.py.

Mục tiêu:
    - Kiểm tra deterministic behavior của Faker + NumPy seed.
    - Kiểm tra business rules của synthetic drivers.
    - Kiểm tra mapping customer zip -> H3 zone.
    - Kiểm tra order -> zone mapping và loại order không map được.
    - Kiểm tra delivery_attempts được neo đúng vào Olist thật.
    - Kiểm tra attempt sequence, timestamp, driver assignment.
    - Kiểm tra failed_reason_id chỉ xuất hiện trên failed attempt.
    - Kiểm tra weather / holiday / commercial event lookup.
    - Không gọi API thật, không cần PostgreSQL.

Đây là UNIT TEST.
E2E với dataset Olist thật sẽ được thực hiện riêng sau khi toàn bộ
unit test suite pass.
"""

from __future__ import annotations

import pandas as pd
import pytest

from ingestion.generators.synthetic_generator import (
    FAILED_REASONS,
    HIRE_DATE_END,
    HIRE_DATE_START,
    SIMULATION_HEAVY_RAIN_MM,
    TIER_RISK_BONUS,
    build_zip_to_zone_map,
    generate_delivery_attempts,
    generate_drivers,
    map_orders_to_zone,
)
from ingestion.utils.geo import latlng_to_h3


# ============================================================================
# Helpers
# ============================================================================

def make_orders_df() -> pd.DataFrame:
    """Tạo orders tối thiểu cho các test delivery_attempts."""
    return pd.DataFrame(
        {
            "order_id": ["O1", "O2", "O3", "O4"],
            "customer_id": ["C1", "C2", "C3", "C4"],
            "order_status": [
                "delivered",
                "delivered",
                "shipped",
                "cancelled",
            ],
            "order_delivered_carrier_date": pd.to_datetime(
                [
                    "2017-01-10",
                    "2017-02-10",
                    "2017-03-10",
                    None,
                ]
            ),
            "order_delivered_customer_date": pd.to_datetime(
                [
                    "2017-01-15",
                    "2017-02-12",
                    None,
                    None,
                ]
            ),
            "order_estimated_delivery_date": pd.to_datetime(
                [
                    "2017-01-13",
                    "2017-02-10",
                    "2017-03-15",
                    None,
                ]
            ),
            "zone_id": [
                "ZONE_A",
                "ZONE_A",
                "ZONE_A",
                "ZONE_A",
            ],
        }
    )


def make_drivers_df() -> pd.DataFrame:
    """
    Driver pool tối thiểu nhưng đủ để test:

        - cùng zone với order
        - active
        - hire_date trước attempt date
    """
    return pd.DataFrame(
        {
            "driver_id": ["DRV0001", "DRV0002"],
            "full_name": ["Driver One", "Driver Two"],
            "vehicle_type": ["motorcycle", "car"],
            "zone_id": ["ZONE_A", "ZONE_A"],
            "hire_date": pd.to_datetime(
                ["2015-06-01", "2015-07-01"]
            ),
            "status": ["active", "active"],
        }
    )


# ============================================================================
# generate_drivers()
# ============================================================================

class TestGenerateDrivers:
    def test_generates_requested_number_of_drivers(self, sample_zone_centroids_df):
        result = generate_drivers(
            sample_zone_centroids_df,
            n_drivers=50,
            seed=42,
        )

        assert len(result.df) == 50
        assert result.table_name == "drivers"
        assert result.metadata["n_drivers"] == 50
        assert result.metadata["seed"] == 42

    def test_driver_ids_are_unique_and_sequential(self, sample_zone_centroids_df):
        result = generate_drivers(
            sample_zone_centroids_df,
            n_drivers=10,
            seed=42,
        )

        assert result.df["driver_id"].is_unique
        assert result.df["driver_id"].tolist() == [
            f"DRV{i:04d}" for i in range(1, 11)
        ]

    def test_driver_output_has_expected_columns(self, sample_zone_centroids_df):
        result = generate_drivers(
            sample_zone_centroids_df,
            n_drivers=20,
            seed=42,
        )

        expected_columns = {
            "driver_id",
            "full_name",
            "vehicle_type",
            "zone_id",
            "hire_date",
            "status",
        }

        assert set(result.df.columns) == expected_columns

    def test_driver_domain_values_are_valid(self, sample_zone_centroids_df):
        result = generate_drivers(
            sample_zone_centroids_df,
            n_drivers=100,
            seed=42,
        )

        assert result.df["vehicle_type"].isin(
            ["motorcycle", "car", "van", "bicycle"]
        ).all()

        assert result.df["status"].isin(
            ["active", "inactive"]
        ).all()

        assert result.df["zone_id"].isin(
            sample_zone_centroids_df["zone_id"]
        ).all()

    def test_driver_hire_dates_are_before_olist_period(
        self,
        sample_zone_centroids_df,
    ):
        result = generate_drivers(
            sample_zone_centroids_df,
            n_drivers=100,
            seed=42,
        )

        assert result.df["hire_date"].min() >= HIRE_DATE_START
        assert result.df["hire_date"].max() < HIRE_DATE_END
        assert result.df["hire_date"].max() < pd.Timestamp("2016-09-01")

    def test_driver_generation_is_deterministic(self, sample_zone_centroids_df):
        result_1 = generate_drivers(
            sample_zone_centroids_df,
            n_drivers=50,
            seed=42,
        )

        result_2 = generate_drivers(
            sample_zone_centroids_df,
            n_drivers=50,
            seed=42,
        )

        pd.testing.assert_frame_equal(
            result_1.df,
            result_2.df,
        )

    def test_different_seed_changes_generated_data(
        self,
        sample_zone_centroids_df,
    ):
        result_1 = generate_drivers(
            sample_zone_centroids_df,
            n_drivers=50,
            seed=42,
        )

        result_2 = generate_drivers(
            sample_zone_centroids_df,
            n_drivers=50,
            seed=99,
        )

        assert not result_1.df.equals(result_2.df)


# ============================================================================
# build_zip_to_zone_map()
# ============================================================================

class TestBuildZipToZoneMap:
    def test_builds_one_zone_per_zip_prefix(self):
        geolocation_df = pd.DataFrame(
            {
                "geolocation_zip_code_prefix": [
                    1000,
                    1000,
                    2000,
                    2000,
                ],
                "geolocation_lat": [
                    -23.5500,
                    -23.5600,
                    -22.9000,
                    -22.9100,
                ],
                "geolocation_lng": [
                    -46.6300,
                    -46.6400,
                    -43.2000,
                    -43.2100,
                ],
            }
        )

        result = build_zip_to_zone_map(geolocation_df)

        assert len(result) == 2
        assert set(result.columns) == {
            "customer_zip_code_prefix",
            "zone_id",
        }

        assert result["customer_zip_code_prefix"].is_unique
        assert result["zone_id"].notna().all()

    def test_zone_id_is_h3_resolution_5(self):
        geolocation_df = pd.DataFrame(
            {
                "geolocation_zip_code_prefix": [1000],
                "geolocation_lat": [-23.5500],
                "geolocation_lng": [-46.6300],
            }
        )

        result = build_zip_to_zone_map(geolocation_df)

        expected_zone = latlng_to_h3(
            -23.5500,
            -46.6300,
            resolution=5,
        )

        assert result.loc[0, "zone_id"] == expected_zone

    def test_multiple_points_of_same_zip_are_averaged(self):
        geolocation_df = pd.DataFrame(
            {
                "geolocation_zip_code_prefix": [1000, 1000],
                "geolocation_lat": [-23.0, -24.0],
                "geolocation_lng": [-46.0, -44.0],
            }
        )

        result = build_zip_to_zone_map(geolocation_df)

        expected_zone = latlng_to_h3(
            -23.5,
            -45.0,
            resolution=5,
        )

        assert result.loc[0, "zone_id"] == expected_zone


# ============================================================================
# map_orders_to_zone()
# ============================================================================

class TestMapOrdersToZone:
    def test_maps_order_through_customer_zip(self):
        orders_df = pd.DataFrame(
            {
                "order_id": ["O1", "O2"],
                "customer_id": ["C1", "C2"],
            }
        )

        customers_df = pd.DataFrame(
            {
                "customer_id": ["C1", "C2"],
                "customer_zip_code_prefix": [1000, 2000],
            }
        )

        zip_to_zone_df = pd.DataFrame(
            {
                "customer_zip_code_prefix": [1000, 2000],
                "zone_id": ["ZONE_A", "ZONE_B"],
            }
        )

        result = map_orders_to_zone(
            orders_df,
            customers_df,
            zip_to_zone_df,
        )

        assert len(result) == 2

        mapping = dict(
            zip(result["order_id"], result["zone_id"])
        )

        assert mapping == {
            "O1": "ZONE_A",
            "O2": "ZONE_B",
        }

    def test_unmapped_orders_are_excluded(self):
        orders_df = pd.DataFrame(
            {
                "order_id": ["O1", "O2"],
                "customer_id": ["C1", "C2"],
            }
        )

        customers_df = pd.DataFrame(
            {
                "customer_id": ["C1", "C2"],
                "customer_zip_code_prefix": [1000, 9999],
            }
        )

        zip_to_zone_df = pd.DataFrame(
            {
                "customer_zip_code_prefix": [1000],
                "zone_id": ["ZONE_A"],
            }
        )

        result = map_orders_to_zone(
            orders_df,
            customers_df,
            zip_to_zone_df,
        )

        assert len(result) == 1
        assert result.iloc[0]["order_id"] == "O1"

    def test_original_order_columns_are_preserved(self):
        orders_df = pd.DataFrame(
            {
                "order_id": ["O1"],
                "customer_id": ["C1"],
                "order_status": ["delivered"],
            }
        )

        customers_df = pd.DataFrame(
            {
                "customer_id": ["C1"],
                "customer_zip_code_prefix": [1000],
            }
        )

        zip_to_zone_df = pd.DataFrame(
            {
                "customer_zip_code_prefix": [1000],
                "zone_id": ["ZONE_A"],
            }
        )

        result = map_orders_to_zone(
            orders_df,
            customers_df,
            zip_to_zone_df,
        )

        assert "order_id" in result.columns
        assert "customer_id" in result.columns
        assert "order_status" in result.columns
        assert "zone_id" in result.columns


# ============================================================================
# generate_delivery_attempts()
# ============================================================================

class TestGenerateDeliveryAttempts:
    def test_output_has_expected_table_name(self):
        orders = make_orders_df()
        drivers = make_drivers_df()

        result = generate_delivery_attempts(
            orders,
            drivers,
            seed=42,
        )

        assert result.table_name == "delivery_attempts"

    def test_orders_without_carrier_date_generate_no_attempts(self):
        orders = pd.DataFrame(
            {
                "order_id": ["O1"],
                "order_status": ["created"],
                "order_delivered_carrier_date": [pd.NaT],
                "order_delivered_customer_date": [pd.NaT],
                "order_estimated_delivery_date": [
                    pd.Timestamp("2017-01-15")
                ],
                "zone_id": ["ZONE_A"],
            }
        )

        result = generate_delivery_attempts(
            orders,
            make_drivers_df(),
            seed=42,
        )

        assert result.df.empty
        assert result.metadata["n_orders_excluded_no_carrier_date"] == 1

    def test_delivered_order_has_final_success_attempt(self):
        orders = pd.DataFrame(
            {
                "order_id": ["O1"],
                "order_status": ["delivered"],
                "order_delivered_carrier_date": [
                    pd.Timestamp("2017-01-10")
                ],
                "order_delivered_customer_date": [
                    pd.Timestamp("2017-01-15")
                ],
                "order_estimated_delivery_date": [
                    pd.Timestamp("2017-01-13")
                ],
                "zone_id": ["ZONE_A"],
            }
        )

        result = generate_delivery_attempts(
            orders,
            make_drivers_df(),
            seed=42,
        )

        attempts = result.df

        assert not attempts.empty

        final_attempt = attempts.iloc[-1]

        assert final_attempt["order_id"] == "O1"
        assert final_attempt["attempt_status"] == "success"
        assert final_attempt["attempt_timestamp"] == pd.Timestamp(
            "2017-01-15"
        )
        assert pd.isna(final_attempt["failed_reason_id"])

    def test_final_attempt_timestamp_matches_real_olist_date(self):
        orders = pd.DataFrame(
            {
                "order_id": ["O1"],
                "order_status": ["delivered"],
                "order_delivered_carrier_date": [
                    pd.Timestamp("2017-05-01")
                ],
                "order_delivered_customer_date": [
                    pd.Timestamp("2017-05-08")
                ],
                "order_estimated_delivery_date": [
                    pd.Timestamp("2017-05-05")
                ],
                "zone_id": ["ZONE_A"],
            }
        )

        result = generate_delivery_attempts(
            orders,
            make_drivers_df(),
            seed=42,
        )

        attempts = result.df

        assert attempts.iloc[-1]["attempt_timestamp"] == pd.Timestamp(
            "2017-05-08"
        )

    def test_non_delivered_order_has_only_failed_attempts(self):
        orders = pd.DataFrame(
            {
                "order_id": ["O1"],
                "order_status": ["shipped"],
                "order_delivered_carrier_date": [
                    pd.Timestamp("2017-05-10")
                ],
                "order_delivered_customer_date": [pd.NaT],
                "order_estimated_delivery_date": [
                    pd.Timestamp("2017-05-15")
                ],
                "zone_id": ["ZONE_A"],
            }
        )

        result = generate_delivery_attempts(
            orders,
            make_drivers_df(),
            seed=42,
        )

        assert not result.df.empty
        assert (result.df["attempt_status"] == "failed").all()
        assert result.df["failed_reason_id"].notna().all()

    def test_attempt_numbers_are_contiguous_per_order(self):
        result = generate_delivery_attempts(
            make_orders_df(),
            make_drivers_df(),
            seed=42,
        )

        for order_id, group in result.df.groupby("order_id"):
            expected = list(range(1, len(group) + 1))

            assert group["attempt_number"].tolist() == expected

    def test_attempt_ids_are_unique(self):
        result = generate_delivery_attempts(
            make_orders_df(),
            make_drivers_df(),
            seed=42,
        )

        assert result.df["attempt_id"].is_unique

    def test_attempt_dates_never_precede_carrier_date(self):
        orders = make_orders_df()

        result = generate_delivery_attempts(
            orders,
            make_drivers_df(),
            seed=42,
        )

        carrier_dates = (
            orders[
                [
                    "order_id",
                    "order_delivered_carrier_date",
                ]
            ]
            .dropna()
            .set_index("order_id")[
                "order_delivered_carrier_date"
            ]
        )

        for _, row in result.df.iterrows():
            carrier_date = carrier_dates[row["order_id"]]

            assert row["attempt_timestamp"] > carrier_date

    def test_driver_is_active_and_same_zone(self):
        orders = make_orders_df()

        result = generate_delivery_attempts(
            orders,
            make_drivers_df(),
            seed=42,
        )

        drivers = make_drivers_df().set_index("driver_id")

        for _, attempt in result.df.iterrows():
            driver = drivers.loc[attempt["driver_id"]]

            assert driver["status"] == "active"
            assert driver["zone_id"] == attempt["zone_id"]

    def test_driver_was_hired_before_attempt(self):
        orders = make_orders_df()

        result = generate_delivery_attempts(
            orders,
            make_drivers_df(),
            seed=42,
        )

        drivers = make_drivers_df().set_index("driver_id")

        for _, attempt in result.df.iterrows():
            driver = drivers.loc[attempt["driver_id"]]

            assert driver["hire_date"] <= attempt["attempt_timestamp"]

    def test_failed_reason_only_exists_on_failed_attempts(self):
        result = generate_delivery_attempts(
            make_orders_df(),
            make_drivers_df(),
            seed=42,
        )

        failed = result.df[
            result.df["attempt_status"] == "failed"
        ]
        successful = result.df[
            result.df["attempt_status"] == "success"
        ]

        assert failed["failed_reason_id"].notna().all()

        if not successful.empty:
            assert successful["failed_reason_id"].isna().all()

    def test_generated_attempts_are_deterministic(self):
        orders = make_orders_df()
        drivers = make_drivers_df()

        result_1 = generate_delivery_attempts(
            orders,
            drivers,
            seed=42,
        )

        result_2 = generate_delivery_attempts(
            orders,
            drivers,
            seed=42,
        )

        pd.testing.assert_frame_equal(
            result_1.df,
            result_2.df,
        )

    def test_different_seed_changes_attempt_simulation(self):
        orders = make_orders_df()
        drivers = make_drivers_df()

        result_1 = generate_delivery_attempts(
            orders,
            drivers,
            seed=42,
        )

        result_2 = generate_delivery_attempts(
            orders,
            drivers,
            seed=99,
        )

        assert not result_1.df.equals(result_2.df)

    def test_metadata_matches_generated_output(self):
        result = generate_delivery_attempts(
            make_orders_df(),
            make_drivers_df(),
            seed=42,
        )

        assert result.metadata["source_name"] == "synthetic"
        assert result.metadata["table_name"] == "delivery_attempts"
        assert result.metadata["n_orders_input"] == 4
        assert result.metadata["n_attempts_generated"] == len(result.df)
        assert result.metadata["seed"] == 42


# ============================================================================
# Risk / lookup helpers
# ============================================================================

class TestGeneratorRiskHelpers:
    def test_heavy_rain_threshold_is_explicit(self):
        """
        Guard against accidental change of the simulation threshold.

        Đây là heuristic riêng của synthetic simulation, không phải
        weather_severity_bucket chính thức trong dbt.
        """
        assert SIMULATION_HEAVY_RAIN_MM == 20.0

    def test_event_risk_bonus_configuration_is_non_negative(self):
        assert TIER_RISK_BONUS["Tier_1_Mega_Boost"] == 0.30
        assert TIER_RISK_BONUS["Tier_2_High_Boost"] == 0.15

        assert all(
            bonus >= 0
            for bonus in TIER_RISK_BONUS.values()
        )

    def test_failed_reason_catalog_has_unique_ids(self):
        ids = [
            reason["failed_reason_id"]
            for reason in FAILED_REASONS
        ]

        assert len(ids) == len(set(ids))

    def test_failed_reason_catalog_has_expected_categories(self):
        categories = {
            reason["category"]
            for reason in FAILED_REASONS
        }

        assert categories == {
            "customer",
            "operations",
            "external",
        }


# ============================================================================
# Weather-conditioned failed_reason (bổ sung — quy tắc nghiệp vụ quan trọng
# nhất đã thống nhất: "thời tiết cực đoan" (FR09) CHỈ được chọn khi weather
# thật xấu tại đúng zone/ngày, KHÔNG phải random thuần)
# ============================================================================

class TestFailedReasonWeatherConditioning:
    def test_fr09_never_chosen_without_severe_weather(self):
        import numpy as np
        from ingestion.generators.synthetic_generator import _pick_failed_reason

        rng = np.random.default_rng(123)
        reasons = [_pick_failed_reason(rng, is_weather_severe=False) for _ in range(3000)]
        assert "FR09" not in reasons

    def test_fr09_can_be_chosen_with_severe_weather(self):
        import numpy as np
        from ingestion.generators.synthetic_generator import _pick_failed_reason

        rng = np.random.default_rng(456)
        reasons = [_pick_failed_reason(rng, is_weather_severe=True) for _ in range(3000)]
        assert "FR09" in reasons

    def test_category_weights_approximate_70_25_5(self):
        import numpy as np
        from ingestion.generators.synthetic_generator import (
            _CUSTOMER_REASONS,
            _OPERATIONS_REASONS,
            _pick_failed_reason,
        )

        rng = np.random.default_rng(789)
        n = 5000
        reasons = [_pick_failed_reason(rng, is_weather_severe=False) for _ in range(n)]

        n_customer = sum(1 for r in reasons if r in _CUSTOMER_REASONS)
        n_ops = sum(1 for r in reasons if r in _OPERATIONS_REASONS)

        assert 0.65 < n_customer / n < 0.75
        assert 0.20 < n_ops / n < 0.30

    def test_severe_weather_at_delivery_date_can_produce_fr09_end_to_end(self):
        """
        Test end-to-end qua generate_delivery_attempts() (không chỉ hàm nội
        bộ _pick_failed_reason) — dựng weather_df có mưa >20mm đúng zone/ngày
        của 1 đơn trễ nặng, chạy nhiều seed để xác nhận FR09 CÓ THỂ xuất hiện.
        """
        orders = pd.DataFrame(
            {
                "order_id": ["O1"],
                "order_status": ["delivered"],
                "order_delivered_carrier_date": [pd.Timestamp("2017-06-01")],
                "order_delivered_customer_date": [pd.Timestamp("2017-06-20")],  # trễ nặng
                "order_estimated_delivery_date": [pd.Timestamp("2017-06-10")],
                "zone_id": ["ZONE_A"],
            }
        )
        # Mưa lớn (>20mm) suốt cả khoảng attempt có thể xảy ra
        weather_df = pd.DataFrame(
            {
                "zone_id": ["ZONE_A"] * 20,
                "date": pd.date_range("2017-06-01", periods=20),
                "precipitation_mm": [35.0] * 20,
            }
        )

        found_fr09 = False
        for seed in range(100):
            result = generate_delivery_attempts(orders, make_drivers_df(), weather_df=weather_df, seed=seed)
            if (result.df["failed_reason_id"] == "FR09").any():
                found_fr09 = True
                break
        assert found_fr09, "FR09 phải có thể xuất hiện khi weather thật xấu tại đúng zone/ngày"

    def test_no_weather_df_disables_severe_weather_reason_end_to_end(self):
        """weather_df=None -> is_weather_severe luôn False -> FR09 không bao giờ xuất hiện."""
        orders = pd.DataFrame(
            {
                "order_id": ["O1"],
                "order_status": ["delivered"],
                "order_delivered_carrier_date": [pd.Timestamp("2017-06-01")],
                "order_delivered_customer_date": [pd.Timestamp("2017-06-20")],
                "order_estimated_delivery_date": [pd.Timestamp("2017-06-10")],
                "zone_id": ["ZONE_A"],
            }
        )
        for seed in range(20):
            result = generate_delivery_attempts(orders, make_drivers_df(), weather_df=None, seed=seed)
            assert (result.df["failed_reason_id"] != "FR09").all()


# ============================================================================
# holiday_impact_tier / commercial_event_tier risk bonus (bổ sung)
# ============================================================================

class TestEventRiskBonus:
    def test_event_risk_bonus_direct(self):
        from ingestion.generators.synthetic_generator import _event_risk_bonus

        holiday_lookup = {pd.Timestamp("2017-11-24"): "Tier_1_Mega_Boost"}
        event_lookup = {pd.Timestamp("2017-03-12"): "Tier_2_High_Boost"}

        assert _event_risk_bonus(pd.Timestamp("2017-11-24"), holiday_lookup, event_lookup) == 0.30
        assert _event_risk_bonus(pd.Timestamp("2017-03-12"), holiday_lookup, event_lookup) == 0.15
        # Ngày neutral / không có trong lookup -> khong co risk bonus
        assert _event_risk_bonus(pd.Timestamp("2017-05-01"), holiday_lookup, event_lookup) == 0.0

    def test_build_date_tier_lookup_reads_seed_file(self, tmp_path):
        from ingestion.generators.synthetic_generator import _build_date_tier_lookup

        seed_path = tmp_path / "holidays.csv"
        pd.DataFrame(
            {
                "date": ["2017-11-24"],
                "local_name": ["Black Friday"],
                "holiday_impact_tier": ["Tier_1_Mega_Boost"],
            }
        ).to_csv(seed_path, index=False)

        lookup = _build_date_tier_lookup(seed_path, "holiday_impact_tier")
        assert lookup[pd.Timestamp("2017-11-24")] == "Tier_1_Mega_Boost"

    def test_missing_seed_file_returns_empty_lookup_gracefully(self, tmp_path):
        from ingestion.generators.synthetic_generator import _build_date_tier_lookup

        lookup = _build_date_tier_lookup(tmp_path / "khong_ton_tai.csv", "holiday_impact_tier")
        assert lookup == {}

    def test_high_tier_date_increases_attempt_failure_rate(self, tmp_path):
        """
        So sánh tỷ lệ đơn CHỈ có 1 attempt (giao trót lọt lần đầu) giữa ngày
        Tier_1_Mega_Boost (rủi ro cao) vs ngày Tier_3_Neutral — kỳ vọng
        Tier_1 có tỷ lệ multi-attempt CAO HƠN rõ rệt qua nhiều seed.
        """
        holidays_path = tmp_path / "holidays.csv"
        pd.DataFrame(columns=["date", "local_name", "holiday_impact_tier"]).to_csv(
            holidays_path, index=False
        )
        events_path = tmp_path / "commercial_events.csv"
        pd.DataFrame(
            {
                "date": ["2017-11-24"],
                "commercial_event_name": ["Black_Friday_Week_2017"],
                "commercial_event_tier": ["Tier_1_Mega_Boost"],
            }
        ).to_csv(events_path, index=False)

        def make_order(delivered_date):
            return pd.DataFrame(
                {
                    "order_id": ["O1"],
                    "order_status": ["delivered"],
                    "order_delivered_carrier_date": [pd.Timestamp(delivered_date) - pd.Timedelta(days=5)],
                    "order_delivered_customer_date": [pd.Timestamp(delivered_date)],
                    "order_estimated_delivery_date": [pd.Timestamp(delivered_date) - pd.Timedelta(days=1)],
                    "zone_id": ["ZONE_A"],
                }
            )

        n_multi_boost = 0
        n_multi_neutral = 0
        trials = 40
        for seed in range(trials):
            r_boost = generate_delivery_attempts(
                make_order("2017-11-24"), make_drivers_df(),
                holidays_seed_path=holidays_path, commercial_events_seed_path=events_path, seed=seed,
            )
            if len(r_boost.df) > 1:
                n_multi_boost += 1

            r_neutral = generate_delivery_attempts(
                make_order("2017-05-15"), make_drivers_df(),
                holidays_seed_path=holidays_path, commercial_events_seed_path=events_path, seed=seed,
            )
            if len(r_neutral.df) > 1:
                n_multi_neutral += 1

        assert n_multi_boost > n_multi_neutral, (
            f"Ky vong Tier_1_Mega_Boost ({n_multi_boost}/{trials} multi-attempt) "
            f"> Tier_3_Neutral ({n_multi_neutral}/{trials})"
        )


# ============================================================================
# Driver fallback khi zone không còn tài xế active hợp lệ (bổ sung)
# ============================================================================

class TestDriverFallback:
    def test_falls_back_to_global_pool_when_zone_has_no_active_driver(self):
        from ingestion.generators.synthetic_generator import _build_driver_lookup, _pick_driver
        import numpy as np

        # ZONE_A không có tài xế nào cả -> phai fallback sang ZONE_B
        drivers_df = pd.DataFrame(
            {
                "driver_id": ["DRV0001"],
                "full_name": ["Driver One"],
                "vehicle_type": ["car"],
                "zone_id": ["ZONE_B"],
                "hire_date": [pd.Timestamp("2015-01-01")],
                "status": ["active"],
            }
        )
        lookup = _build_driver_lookup(drivers_df)
        rng = np.random.default_rng(1)

        chosen = _pick_driver("ZONE_A", pd.Timestamp("2017-06-01"), lookup, rng)
        assert chosen == "DRV0001"  # fallback đúng sang pool toàn cục

    def test_returns_none_when_absolutely_no_active_driver_exists(self):
        from ingestion.generators.synthetic_generator import _build_driver_lookup, _pick_driver
        import numpy as np

        drivers_df = pd.DataFrame(
            {
                "driver_id": ["DRV0001"],
                "full_name": ["Driver One"],
                "vehicle_type": ["car"],
                "zone_id": ["ZONE_A"],
                "hire_date": [pd.Timestamp("2020-01-01")],  # tuyển SAU ngày attempt
                "status": ["active"],
            }
        )
        lookup = _build_driver_lookup(drivers_df)
        rng = np.random.default_rng(1)

        chosen = _pick_driver("ZONE_A", pd.Timestamp("2017-06-01"), lookup, rng)
        assert chosen is None