"""
Test cho phiên bản mới của synthetic_generator.py — chỉ bao phần THAY ĐỔI/THÊM MỚI:
    - danh mục failed_reason khớp dbt seed, CATEGORY_WEIGHTS thật sự được dùng
    - weather lookup vector hóa (ngữ nghĩa giống bản iterrows cũ)
    - ưu tiên Commercial Event > Public Holiday, bỏ NaN tier
    - chọn driver 3 bậc (zone -> state -> global)
    - zone lấy từ seed zip_zone_mapping (giữ số 0 đầu, raise khi seed hỏng / nhân dòng)
    - validate_delivery_attempts() bắt được dữ liệu sai
    - end-to-end: kết quả tất định, neo đúng Olist, metadata JSON-safe
Không cần Postgres/API thật. Test cũ trong test_generators.py: xem ghi chú bàn giao.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from ingestion.generators import synthetic_generator as sg

REPO_ROOT = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# Dữ liệu mẫu nhỏ
# ---------------------------------------------------------------------------
def _make_zones(n: int = 40) -> pd.DataFrame:
    rng = np.random.default_rng(11)
    return pd.DataFrame(
        {
            "zone_id": [f"z{i:03d}" for i in range(n)],
            "point_count": rng.integers(1, 200, n),
            "dominant_state": rng.choice(["SP", "RJ", "MG"], n),
        }
    )


def _make_orders(zones: pd.DataFrame, n: int = 500, seed: int = 5) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    purchase = pd.Timestamp("2017-03-01") + pd.to_timedelta(rng.integers(0, 300, n), unit="D")
    carrier = purchase + pd.to_timedelta(rng.integers(1, 5, n), unit="D")
    delivered = (
        carrier
        + pd.to_timedelta(rng.integers(1, 20, n), unit="D")
        + pd.to_timedelta(rng.integers(0, 86400, n), unit="s")
    )
    df = pd.DataFrame(
        {
            "order_id": [f"o{i:05d}" for i in range(n)],
            "customer_id": [f"c{i:05d}" for i in range(n)],
            "order_status": rng.choice(["delivered", "shipped", "canceled"], n, p=[0.88, 0.07, 0.05]),
            "order_delivered_carrier_date": carrier,
            "order_delivered_customer_date": delivered,
            "order_estimated_delivery_date": purchase + pd.to_timedelta(rng.integers(8, 25, n), unit="D"),
            "zone_id": rng.choice(zones["zone_id"], n),
        }
    )
    df.loc[rng.random(n) < 0.05, "order_delivered_carrier_date"] = pd.NaT
    df.loc[rng.random(n) < 0.02, "order_delivered_customer_date"] = pd.NaT
    df.loc[df["order_status"] != "delivered", "order_delivered_customer_date"] = pd.NaT
    return df


def _make_weather(zones: pd.DataFrame) -> pd.DataFrame:
    days = pd.date_range("2017-02-01", "2018-03-01")
    grid = pd.MultiIndex.from_product([zones["zone_id"], days], names=["zone_id", "date"]).to_frame(index=False)
    rng = np.random.default_rng(3)
    grid["precipitation_mm"] = np.where(rng.random(len(grid)) < 0.12, 45.0, 2.0)
    return grid


def _write_seeds(tmp_path: Path) -> tuple[Path, Path]:
    hp, ep = tmp_path / "holidays.csv", tmp_path / "events.csv"
    pd.DataFrame(
        {"date": ["2017-04-21", "2017-09-07"], "holiday_impact_tier": ["Tier_1_Mega_Boost", "Tier_3_Neutral"]}
    ).to_csv(hp, index=False)
    pd.DataFrame(
        {"date": ["2017-11-24"], "commercial_event_tier": ["Tier_1_Mega_Boost"]}
    ).to_csv(ep, index=False)
    return hp, ep


@pytest.fixture
def world(tmp_path):
    zones = _make_zones()
    drivers = sg.generate_drivers(zones, n_drivers=60, seed=42).df
    hp, ep = _write_seeds(tmp_path)
    return {
        "zones": zones,
        "drivers": drivers,
        "orders": _make_orders(zones),
        "weather": _make_weather(zones),
        "holidays": hp,
        "events": ep,
    }


def _run(world, **overrides):
    kwargs = dict(
        weather_df=world["weather"],
        holidays_seed_path=world["holidays"],
        commercial_events_seed_path=world["events"],
        seed=42,
    )
    kwargs.update(overrides)
    return sg.generate_delivery_attempts(world["orders"], world["drivers"], **kwargs)


# ---------------------------------------------------------------------------
# Hằng số / danh mục
# ---------------------------------------------------------------------------
def test_failed_reason_catalog_matches_dbt_seed():
    seed_path = REPO_ROOT / "dbt" / "seeds" / "failed_reasons.csv"
    if not seed_path.exists():
        pytest.skip(f"{seed_path} not found")
    seed = pd.read_csv(seed_path, dtype=str)
    col = lambda *names: next(n for n in names if n in seed.columns)  # noqa: E731
    seed_rows = {
        (r[col("failed_reason_id")].strip(), r[col("failed_reason_category", "category")].strip(),
         r[col("failed_reason_name", "reason_name")].strip())
        for _, r in seed.iterrows()
    }
    gen_rows = {(r["failed_reason_id"], r["category"], r["reason_name"]) for r in sg.FAILED_REASONS}
    assert gen_rows == seed_rows


def test_check_constants_rejects_weights_not_summing_to_one(monkeypatch):
    monkeypatch.setattr(sg, "CATEGORY_WEIGHTS", {"customer": 0.5, "operations": 0.25, "external": 0.05})
    with pytest.raises(ValueError, match="sum to 1.0"):
        sg._check_constants()


def test_pick_failed_reason_actually_uses_category_weights(monkeypatch):
    monkeypatch.setattr(sg, "CATEGORY_WEIGHTS", {"customer": 0.0, "operations": 1.0, "external": 0.0})
    rng = np.random.default_rng(0)
    picks = {sg._pick_failed_reason(rng, False) for _ in range(200)}
    assert picks <= set(sg._OPERATIONS_REASONS) and picks


def test_fr09_never_picked_without_severe_weather():
    rng = np.random.default_rng(1)
    picks = [sg._pick_failed_reason(rng, False) for _ in range(3000)]
    assert sg.REASON_EXTREME_WEATHER not in picks
    picks_severe = [sg._pick_failed_reason(rng, True) for _ in range(3000)]
    assert sg.REASON_EXTREME_WEATHER in picks_severe


# ---------------------------------------------------------------------------
# Weather lookup
# ---------------------------------------------------------------------------
def _reference_weather_lookup(weather_df):
    """Bản iterrows cũ — dùng làm đối chứng ngữ nghĩa."""
    lookup = {}
    for _, row in weather_df.iterrows():
        key = (row["zone_id"], pd.Timestamp(row["date"]).normalize())
        lookup[key] = row["precipitation_mm"] > sg.SIMULATION_HEAVY_RAIN_MM
    return lookup


def test_weather_lookup_matches_reference_semantics():
    rng = np.random.default_rng(2)
    n = 3000
    w = pd.DataFrame(
        {
            "zone_id": rng.choice(["a", "b", "c"], n),
            "date": pd.to_datetime("2017-01-01") + pd.to_timedelta(rng.integers(0, 40, n), unit="D"),
            "precipitation_mm": rng.uniform(0, 60, n),
        }
    )
    w.loc[rng.random(n) < 0.05, "precipitation_mm"] = np.nan
    w.loc[rng.random(n) < 0.05, "precipitation_mm"] = 20.0  # đúng ngưỡng: KHÔNG phải mưa lớn
    w = pd.concat([w, w.head(300)], ignore_index=True)  # trùng (zone, date), kể cả trùng khác giá trị

    got = sg._build_weather_severity_lookup(w)
    ref = _reference_weather_lookup(w)
    assert all(got.get(k, False) == v for k, v in ref.items())
    assert set(got) == {k for k, v in ref.items() if v}


def test_weather_lookup_empty_inputs():
    assert sg._build_weather_severity_lookup(None) == {}
    assert sg._build_weather_severity_lookup(pd.DataFrame(columns=["zone_id", "date", "precipitation_mm"])) == {}


# ---------------------------------------------------------------------------
# Holiday / commercial event
# ---------------------------------------------------------------------------
def test_commercial_event_takes_priority_over_holiday():
    d = pd.Timestamp("2017-12-25")
    holiday = {d: "Tier_1_Mega_Boost"}
    event = {d: "Tier_3_Neutral"}
    assert sg._event_risk_bonus(d, holiday, event) == 0.0  # event thắng, dù holiday là Mega_Boost
    assert sg._event_risk_bonus(d, holiday, {}) == sg.TIER_RISK_BONUS["Tier_1_Mega_Boost"]
    assert sg._event_risk_bonus(d, {}, {d: "Tier_2_High_Boost"}) == sg.TIER_RISK_BONUS["Tier_2_High_Boost"]
    assert sg._event_risk_bonus(pd.Timestamp("2017-01-01"), holiday, event) == 0.0


def test_date_tier_lookup_drops_nan_tiers(tmp_path):
    p = tmp_path / "events.csv"
    pd.DataFrame(
        {"date": ["2017-11-24", "2017-11-25"], "commercial_event_tier": ["Tier_1_Mega_Boost", np.nan]}
    ).to_csv(p, index=False)
    lookup = sg._build_date_tier_lookup(p, "commercial_event_tier")
    assert list(lookup) == [pd.Timestamp("2017-11-24")]


def test_date_tier_lookup_missing_seed_returns_empty(tmp_path):
    assert sg._build_date_tier_lookup(tmp_path / "nope.csv", "holiday_impact_tier") == {}


# ---------------------------------------------------------------------------
# Chọn driver
# ---------------------------------------------------------------------------
def _driver_fixture():
    hired = pd.Timestamp("2016-01-01")
    late = pd.Timestamp("2017-06-01")
    drivers = pd.DataFrame(
        {
            "driver_id": ["D1", "D2", "D3", "D4"],
            "zone_id": ["z1", "z2", "z3", "z4"],
            "hire_date": [hired, hired, hired, late],
            "status": ["active", "active", "active", "active"],
        }
    )
    zone_state = {"z1": "SP", "z2": "SP", "z3": "RJ", "z4": "RJ", "z9": "SP", "z8": "AM"}
    return drivers, zone_state


def test_pick_driver_three_tiers():
    drivers, zone_state = _driver_fixture()
    lookup = sg._build_driver_lookup(drivers)
    state_lookup = sg._build_state_driver_lookup(drivers, zone_state)
    rng = np.random.default_rng(0)
    date = pd.Timestamp("2017-03-01")

    assert sg._pick_driver_tiered("z1", date, lookup, rng, state_lookup, zone_state) == ("D1", sg.DRIVER_TIER_ZONE)
    # z9 không có tài xế nhưng cùng bang SP với D1/D2 -> bậc state
    driver, tier = sg._pick_driver_tiered("z9", date, lookup, rng, state_lookup, zone_state)
    assert tier == sg.DRIVER_TIER_STATE and driver in {"D1", "D2"}
    # z8 (bang AM) không có tài xế nào cùng bang -> bậc global
    driver, tier = sg._pick_driver_tiered("z8", date, lookup, rng, state_lookup, zone_state)
    assert tier == sg.DRIVER_TIER_GLOBAL and driver in {"D1", "D2", "D3"}


def test_pick_driver_respects_hire_date_and_returns_none_when_empty():
    drivers, zone_state = _driver_fixture()
    lookup = sg._build_driver_lookup(drivers)
    rng = np.random.default_rng(0)
    # D4 (z4) chưa được tuyển vào 2017-03-01 -> rơi xuống global, không bao giờ là D4
    for _ in range(50):
        driver, tier = sg._pick_driver_tiered("z4", pd.Timestamp("2017-03-01"), lookup, rng)
        assert driver != "D4" and tier == sg.DRIVER_TIER_GLOBAL
    assert sg._pick_driver_tiered("z1", pd.Timestamp("2010-01-01"), lookup, rng) == (None, sg.DRIVER_TIER_NONE)


def test_pick_driver_without_state_args_is_two_tier_like_before():
    drivers, zone_state = _driver_fixture()
    lookup = sg._build_driver_lookup(drivers)
    rng = np.random.default_rng(0)
    _driver, tier = sg._pick_driver_tiered("z9", pd.Timestamp("2017-03-01"), lookup, rng)
    assert tier == sg.DRIVER_TIER_GLOBAL  # không truyền state -> bỏ qua bậc bang


def test_inactive_drivers_are_never_pooled():
    drivers, zone_state = _driver_fixture()
    drivers.loc[0, "status"] = "inactive"
    assert "D1" not in {d for pool in sg._build_driver_lookup(drivers).values() for d, _ in pool}
    assert "D1" not in {d for pool in sg._build_state_driver_lookup(drivers, zone_state).values() for d, _ in pool}


# ---------------------------------------------------------------------------
# zip -> zone
# ---------------------------------------------------------------------------
def test_normalize_zip_pads_and_handles_types():
    s = pd.Series([1001, "1001", "01001", "1001.0", np.nan, 99999])
    out = sg._normalize_zip(s).tolist()
    assert out[:4] == ["01001"] * 4
    assert pd.isna(out[4])
    assert out[5] == "99999"


def test_load_zip_to_zone_map_keeps_leading_zeros(tmp_path):
    p = tmp_path / "zip_zone_mapping.csv"
    p.write_text("zip_code_prefix,zone_id\n01001,zA\n1002,zB\n99999,zC\n")
    m = sg.load_zip_to_zone_map(p)
    assert dict(zip(m["customer_zip_code_prefix"], m["zone_id"])) == {"01001": "zA", "01002": "zB", "99999": "zC"}


def test_load_zip_to_zone_map_raises_on_conflicting_zone(tmp_path):
    p = tmp_path / "zip_zone_mapping.csv"
    p.write_text("zip_code_prefix,zone_id\n01001,zA\n1001,zB\n")
    with pytest.raises(ValueError, match="more than one zone_id"):
        sg.load_zip_to_zone_map(p)


def test_load_zip_to_zone_map_tolerates_exact_duplicate_rows(tmp_path):
    p = tmp_path / "zip_zone_mapping.csv"
    p.write_text("zip_code_prefix,zone_id\n01001,zA\n1001,zA\n")
    assert len(sg.load_zip_to_zone_map(p)) == 1


def test_load_zip_to_zone_map_requires_columns(tmp_path):
    p = tmp_path / "zip_zone_mapping.csv"
    p.write_text("zip,zone\n01001,zA\n")
    with pytest.raises(ValueError, match="missing column"):
        sg.load_zip_to_zone_map(p)


def test_map_orders_to_zone_joins_int_and_str_zips_and_drops_unmapped():
    orders = pd.DataFrame({"order_id": ["o1", "o2", "o3"], "customer_id": ["c1", "c2", "c3"]})
    customers = pd.DataFrame(
        {"customer_id": ["c1", "c2", "c3"], "customer_zip_code_prefix": [1001, 1002, 77777]}  # int, mất số 0 đầu
    )
    mapping = pd.DataFrame({"customer_zip_code_prefix": ["01001", "01002"], "zone_id": ["zA", "zB"]})
    out = sg.map_orders_to_zone(orders, customers, mapping)
    assert dict(zip(out["order_id"], out["zone_id"])) == {"o1": "zA", "o2": "zB"}


def test_map_orders_to_zone_raises_instead_of_fanning_out():
    orders = pd.DataFrame({"order_id": ["o1"], "customer_id": ["c1"]})
    dup_customers = pd.DataFrame({"customer_id": ["c1", "c1"], "customer_zip_code_prefix": ["01001", "01001"]})
    mapping = pd.DataFrame({"customer_zip_code_prefix": ["01001"], "zone_id": ["zA"]})
    with pytest.raises(ValueError, match="changed row count"):
        sg.map_orders_to_zone(orders, dup_customers, mapping)


def test_build_zone_state_map():
    zones = pd.DataFrame({"zone_id": ["a", "b", "c"], "dominant_state": ["SP", None, "RJ"]})
    assert sg.build_zone_state_map(zones) == {"a": "SP", "c": "RJ"}
    assert sg.build_zone_state_map(zones.drop(columns="dominant_state")) == {}


# ---------------------------------------------------------------------------
# End-to-end
# ---------------------------------------------------------------------------
def test_generate_delivery_attempts_is_deterministic_and_anchored(world):
    a = _run(world)
    b = _run(world)
    pd.testing.assert_frame_equal(a.df, b.df)
    assert list(a.df.columns) == sg.ATTEMPT_COLUMNS

    df = a.df
    o = world["orders"].set_index("order_id")
    # quy tắc #1: đơn chưa rời kho không có attempt
    no_carrier = set(o.index[o["order_delivered_carrier_date"].isna()])
    assert not (set(df["order_id"]) & no_carrier)
    # quy tắc #2a/#2b: attempt cuối success <=> delivered + có customer_date, và khớp timestamp
    last = df.sort_values(["order_id", "attempt_number"]).groupby("order_id").tail(1).set_index("order_id")
    delivered_ok = (o["order_status"] == "delivered") & o["order_delivered_customer_date"].notna()
    for oid, row in last.iterrows():
        assert (row["attempt_status"] == "success") == bool(delivered_ok[oid])
        if row["attempt_status"] == "success":
            assert row["attempt_timestamp"] == o.loc[oid, "order_delivered_customer_date"]
    # có cả attempt fail lẫn success (tránh test "đúng vì rỗng")
    assert {"success", "failed"} <= set(df["attempt_status"])


def test_generate_delivery_attempts_metadata_is_json_serializable(world):
    res = _run(world, zone_state_map=sg.build_zone_state_map(world["zones"]))
    meta = json.loads(json.dumps(res.metadata))
    assert meta["simulation_params"]["heavy_rain_mm"] == sg.SIMULATION_HEAVY_RAIN_MM
    assert meta["n_attempts_generated"] == len(res.df)
    da = meta["driver_assignment"]
    assert da["zone"] + da["state"] + da["global"] + da["none"] == len(res.df)
    assert da["state_fallback_enabled"] is True


def test_state_fallback_only_changes_driver_and_never_below_zone_tier(world):
    without = _run(world)
    with_state = _run(world, zone_state_map=sg.build_zone_state_map(world["zones"]))
    # cùng attempt, cùng ngày, cùng trạng thái — chỉ driver_id (và các lần rút RNG sau đó) có thể khác
    assert len(without.df) > 0 and len(with_state.df) > 0
    same_zone_a = without.metadata["driver_assignment"]["pct_same_zone"]
    same_zone_b = with_state.metadata["driver_assignment"]["pct_same_zone"]
    assert abs(same_zone_a - same_zone_b) < 0.05  # bậc 1 không bị bậc state làm thay đổi đáng kể
    assert with_state.metadata["driver_assignment"]["state"] > 0


def test_generation_without_weather_and_seeds_still_valid(world, tmp_path):
    res = sg.generate_delivery_attempts(
        world["orders"],
        world["drivers"],
        weather_df=None,
        holidays_seed_path=tmp_path / "missing_h.csv",
        commercial_events_seed_path=tmp_path / "missing_e.csv",
    )
    assert sg.REASON_EXTREME_WEATHER not in set(res.df["failed_reason_id"].dropna())
    assert res.metadata["n_severe_weather_zone_days"] == 0


def test_no_attempts_yields_empty_frame_with_columns(world):
    orders = world["orders"].copy()
    orders["order_delivered_carrier_date"] = pd.NaT
    res = sg.generate_delivery_attempts(orders, world["drivers"], weather_df=None)
    assert res.df.empty and list(res.df.columns) == sg.ATTEMPT_COLUMNS


# ---------------------------------------------------------------------------
# validate_delivery_attempts phải BẮT được dữ liệu sai
# ---------------------------------------------------------------------------
def _validation_inputs(world):
    res = _run(world)
    orders = world["orders"].copy()
    for c in ("order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"):
        orders[c] = pd.to_datetime(orders[c])
    lookup = sg._build_weather_severity_lookup(world["weather"])
    return res.df.copy(), orders, world["drivers"].copy(), lookup


def test_validator_passes_on_clean_output(world):
    attempts, orders, drivers, lookup = _validation_inputs(world)
    stats = sg.validate_delivery_attempts(attempts, orders, drivers, lookup)
    assert stats["n_attempts"] == len(attempts)


def _multi_attempt_order(attempts):
    counts = attempts.groupby("order_id").size()
    return counts[counts >= 2].index[0]


def test_validator_catches_duplicate_attempt_id(world):
    attempts, orders, drivers, lookup = _validation_inputs(world)
    attempts.loc[attempts.index[1], "attempt_id"] = attempts.loc[attempts.index[0], "attempt_id"]
    with pytest.raises(ValueError, match="duplicate attempt_id"):
        sg.validate_delivery_attempts(attempts, orders, drivers, lookup)


def test_validator_catches_success_before_last_attempt(world):
    attempts, orders, drivers, lookup = _validation_inputs(world)
    oid = _multi_attempt_order(attempts)
    first = attempts[(attempts["order_id"] == oid) & (attempts["attempt_number"] == 1)].index[0]
    attempts.loc[first, ["attempt_status", "failed_reason_id"]] = ["success", None]
    with pytest.raises(ValueError, match="not the last attempt"):
        sg.validate_delivery_attempts(attempts, orders, drivers, lookup)


def test_validator_catches_fr09_without_heavy_rain(world):
    attempts, orders, drivers, lookup = _validation_inputs(world)
    failed = attempts[attempts["attempt_status"] == "failed"]
    dry = failed[
        [(z, t.normalize()) not in lookup for z, t in zip(failed["zone_id"], failed["attempt_timestamp"])]
    ]
    attempts.loc[dry.index[0], "failed_reason_id"] = sg.REASON_EXTREME_WEATHER
    with pytest.raises(ValueError, match="FR09"):
        sg.validate_delivery_attempts(attempts, orders, drivers, lookup)


def test_validator_catches_driver_hired_after_attempt(world):
    attempts, orders, drivers, lookup = _validation_inputs(world)
    victim = attempts["driver_id"].dropna().iloc[0]
    drivers.loc[drivers["driver_id"] == victim, "hire_date"] = pd.Timestamp("2030-01-01")
    with pytest.raises(ValueError, match="hired after"):
        sg.validate_delivery_attempts(attempts, orders, drivers, lookup)


def test_validator_catches_outcome_disagreeing_with_order_status(world):
    attempts, orders, drivers, lookup = _validation_inputs(world)
    oid = attempts.loc[attempts["attempt_status"] == "success", "order_id"].iloc[0]
    orders.loc[orders["order_id"] == oid, "order_status"] = "canceled"
    with pytest.raises(ValueError, match="rule 2a"):
        sg.validate_delivery_attempts(attempts, orders, drivers, lookup)


def test_validator_catches_attempt_for_order_that_never_shipped(world):
    attempts, orders, drivers, lookup = _validation_inputs(world)
    oid = attempts["order_id"].iloc[0]
    orders.loc[orders["order_id"] == oid, "order_delivered_carrier_date"] = pd.NaT
    with pytest.raises(ValueError, match="rule 1"):
        sg.validate_delivery_attempts(attempts, orders, drivers, lookup)


# ---------------------------------------------------------------------------
# main(): dùng seed zip_zone_mapping, không cần geolocation
# ---------------------------------------------------------------------------
def test_main_uses_zip_zone_mapping_seed_and_ignores_geolocation(world, tmp_path):
    zones = world["zones"]
    zones.to_csv(tmp_path / "zone_centroids.csv", index=False)

    # zip nguồn ở dạng số (mất số 0 đầu) — seed dùng chuỗi 5 ký tự
    zips = [1000 + i for i in range(len(zones))]
    pd.DataFrame(
        {"zip_code_prefix": [f"{z:05d}" for z in zips], "zone_id": zones["zone_id"]}
    ).to_csv(tmp_path / "zip_zone_mapping.csv", index=False)

    orders = world["orders"].drop(columns="zone_id")
    zip_by_zone = dict(zip(zones["zone_id"], zips))
    customers = pd.DataFrame(
        {
            "customer_id": orders["customer_id"],
            "customer_zip_code_prefix": world["orders"]["zone_id"].map(zip_by_zone),
        }
    )

    drivers_res, attempts_res = sg.main(
        zone_centroids_path=tmp_path / "zone_centroids.csv",
        zip_zone_mapping_path=tmp_path / "zip_zone_mapping.csv",
        holidays_seed_path=world["holidays"],
        commercial_events_seed_path=world["events"],
        orders_df=orders,
        customers_df=customers,
        weather_df=world["weather"],
        geolocation_df=pd.DataFrame(),  # DEPRECATED: bị bỏ qua, không được làm vỡ luồng cũ
        n_drivers=60,
    )
    assert len(drivers_res.df) == 60
    # zone của attempt phải đúng bằng zone gốc (qua seed), không phải zone tính từ toạ độ
    expected_zone = world["orders"].set_index("order_id")["zone_id"]
    got_zone = attempts_res.df.drop_duplicates("order_id").set_index("order_id")["zone_id"]
    assert (got_zone == expected_zone.loc[got_zone.index]).all()
    assert attempts_res.metadata["driver_assignment"]["state_fallback_enabled"] is True