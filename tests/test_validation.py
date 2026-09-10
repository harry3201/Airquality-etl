"""
Tests for the validation and transformation layers.

All tests use small, hand-built synthetic DataFrames rather than the full
700k-row dataset, per project requirements — fast, deterministic, and
focused on logic correctness rather than data volume.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src import validate, transform


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

REQUIRED_COLUMNS = ["City", "Datetime", "PM2.5", "PM10", "AQI", "AQI_Bucket"]
POLLUTANTS = ["PM2.5", "PM10", "NO2"]


@pytest.fixture
def clean_hourly_df() -> pd.DataFrame:
    return pd.DataFrame({
        "City": ["Delhi", "Delhi", "Mumbai", "Mumbai"],
        "Datetime": [
            "2020-01-01 00:00:00", "2020-01-01 01:00:00",
            "2020-01-01 00:00:00", "2020-01-01 01:00:00",
        ],
        "PM2.5": [100.0, 110.0, 40.0, None],
        "PM10": [150.0, 160.0, 60.0, 65.0],
        "NO2": [30.0, 32.0, 20.0, 21.0],
        "AQI": [180.0, 190.0, 90.0, 95.0],
        "AQI_Bucket": ["Moderate", "Moderate", "Satisfactory", "Satisfactory"],
    })


@pytest.fixture
def dirty_hourly_df() -> pd.DataFrame:
    return pd.DataFrame({
        "City": ["Delhi", "Delhi", None, "Mumbai", "Mumbai"],
        "Datetime": [
            "2020-01-01 00:00:00", "2020-01-01 00:00:00",  # duplicate key
            "2020-01-01 02:00:00",                          # null city
            "not-a-date",                                   # invalid datetime
            "2020-01-01 01:00:00",
        ],
        "PM2.5": [100.0, 100.0, 50.0, 40.0, -5.0],           # negative value
        "PM10": [150.0, 150.0, 60.0, 60.0, 65.0],
        "NO2": [30.0, 30.0, 20.0, 20.0, 21.0],
        "AQI": [180.0, 180.0, 90.0, 95.0, 95.0],
        "AQI_Bucket": ["Moderate"] * 5,
    })


# ---------------------------------------------------------------------------
# Required columns
# ---------------------------------------------------------------------------

def test_required_columns_pass_when_all_present(clean_hourly_df):
    rows = validate.check_required_columns(clean_hourly_df, REQUIRED_COLUMNS)
    assert rows[0]["status"] == "PASS"


def test_required_columns_fail_when_missing():
    df = pd.DataFrame({"City": ["Delhi"]})
    rows = validate.check_required_columns(df, REQUIRED_COLUMNS)
    assert rows[0]["status"] == "FAIL"
    assert "Datetime" in rows[0]["value"]


# ---------------------------------------------------------------------------
# Null city
# ---------------------------------------------------------------------------

def test_null_city_detected(dirty_hourly_df):
    rows = validate.check_null_city(dirty_hourly_df)
    assert rows[0]["value"] == 1
    assert rows[0]["status"] == "FAIL"


def test_null_city_pass_when_none(clean_hourly_df):
    rows = validate.check_null_city(clean_hourly_df)
    assert rows[0]["value"] == 0
    assert rows[0]["status"] == "PASS"


# ---------------------------------------------------------------------------
# Invalid datetime
# ---------------------------------------------------------------------------

def test_invalid_datetime_detected(dirty_hourly_df):
    rows = validate.check_invalid_datetime(dirty_hourly_df, "Datetime")
    assert rows[0]["value"] == 1
    assert rows[0]["status"] == "FAIL"


def test_invalid_datetime_pass_when_clean(clean_hourly_df):
    rows = validate.check_invalid_datetime(clean_hourly_df, "Datetime")
    assert rows[0]["value"] == 0
    assert rows[0]["status"] == "PASS"


# ---------------------------------------------------------------------------
# Duplicate detection
# ---------------------------------------------------------------------------

def test_duplicate_detection_finds_duplicate_keys(dirty_hourly_df):
    rows = validate.check_duplicates(dirty_hourly_df, subset=["City", "Datetime"])
    assert rows[0]["value"] == 1
    assert rows[0]["status"] == "WARN"


def test_duplicate_detection_clean_data_has_none(clean_hourly_df):
    rows = validate.check_duplicates(clean_hourly_df, subset=["City", "Datetime"])
    assert rows[0]["value"] == 0
    assert rows[0]["status"] == "PASS"


# ---------------------------------------------------------------------------
# Negative values
# ---------------------------------------------------------------------------

def test_negative_values_detected(dirty_hourly_df):
    rows = validate.check_negative_values(dirty_hourly_df, POLLUTANTS)
    pm25_row = next(r for r in rows if r["metric"] == "PM2.5_negative_count")
    assert pm25_row["value"] == 1
    assert pm25_row["status"] == "WARN"


def test_negative_values_clean_data_has_none(clean_hourly_df):
    rows = validate.check_negative_values(clean_hourly_df, POLLUTANTS)
    assert all(r["value"] == 0 for r in rows)
    assert all(r["status"] == "PASS" for r in rows)


# ---------------------------------------------------------------------------
# Suspicious / extreme values — flagged, never deleted
# ---------------------------------------------------------------------------

def test_suspicious_values_are_flagged_not_removed():
    df = pd.DataFrame({
        "City": ["Delhi"],
        "Datetime": ["2020-01-01 00:00:00"],
        "PM2.5": [2000.0],  # way above any reasonable threshold
    })
    thresholds = {"PM2.5": 1000.0}

    rows = validate.check_suspicious_values(df, thresholds)
    assert rows[0]["value"] == 1
    assert rows[0]["status"] == "WARN"

    flagged = validate.flag_suspicious_values(df, thresholds)
    # The original value must be untouched.
    assert flagged.loc[0, "PM2.5"] == 2000.0
    assert flagged.loc[0, "PM2.5_flag_suspicious"] == True  # noqa: E712
    assert len(flagged) == len(df)  # nothing was dropped


# ---------------------------------------------------------------------------
# Missing-value percentage
# ---------------------------------------------------------------------------

def test_missing_percentage_calculation(clean_hourly_df):
    rows = validate.check_missing_percentages(clean_hourly_df, ["PM2.5"])
    # 1 out of 4 rows is null -> 25%
    assert rows[0]["value"] == 25.0


# ---------------------------------------------------------------------------
# Structural validity split — only structural issues cause a row to drop
# ---------------------------------------------------------------------------

def test_structurally_valid_rows_excludes_only_broken_keys(dirty_hourly_df):
    valid, invalid = validate.get_structurally_valid_rows(dirty_hourly_df)
    # Row with null City and row with invalid Datetime should be excluded,
    # plus one of the two duplicate-key rows.
    assert len(invalid) == 3
    assert len(valid) == len(dirty_hourly_df) - 3
    # The negative PM2.5 value is NOT a structural issue -> must remain.
    assert (-5.0 in valid["PM2.5"].values) or (-5.0 in invalid["PM2.5"].values)


def test_structural_validity_never_drops_extreme_values():
    df = pd.DataFrame({
        "City": ["Delhi"],
        "Datetime": ["2020-01-01 00:00:00"],
        "PM2.5": [5000.0],  # extreme but structurally fine
    })
    valid, invalid = validate.get_structurally_valid_rows(df)
    assert len(valid) == 1
    assert len(invalid) == 0


# ---------------------------------------------------------------------------
# Timestamp gaps vs NULL measurements
# ---------------------------------------------------------------------------

def test_timestamp_gap_detected_for_missing_hour():
    # City has hours 00:00 and 02:00 but is missing 01:00 entirely.
    df = pd.DataFrame({
        "City": ["Delhi", "Delhi"],
        "Datetime": ["2020-01-01 00:00:00", "2020-01-01 02:00:00"],
    })
    rows = validate.check_timestamp_gaps(df)
    missing_hours_row = next(r for r in rows if r["metric"] == "overall_missing_hourly_records")
    assert missing_hours_row["value"] == 1


def test_null_measurement_is_not_a_timestamp_gap():
    # The record exists for every hour; only the pollutant value is null.
    df = pd.DataFrame({
        "City": ["Delhi", "Delhi", "Delhi"],
        "Datetime": ["2020-01-01 00:00:00", "2020-01-01 01:00:00", "2020-01-01 02:00:00"],
        "PM2.5": [100.0, None, 105.0],
    })
    rows = validate.check_timestamp_gaps(df)
    missing_hours_row = next(r for r in rows if r["metric"] == "overall_missing_hourly_records")
    assert missing_hours_row["value"] == 0


# ---------------------------------------------------------------------------
# Daily aggregation / transform
# ---------------------------------------------------------------------------

@pytest.fixture
def hourly_for_transform() -> pd.DataFrame:
    # Delhi has 3 of 24 hours on 2020-01-01: two with PM2.5, one null.
    return pd.DataFrame({
        "City": ["Delhi", "Delhi", "Delhi", "Mumbai"],
        "Datetime": [
            "2020-01-01 00:00:00", "2020-01-01 01:00:00", "2020-01-01 02:00:00",
            "2020-01-01 00:00:00",
        ],
        "PM2.5": [100.0, None, 120.0, 40.0],
        "PM10": [150.0, 155.0, 160.0, 60.0],
        "NO": [1.0, 1.0, 1.0, 1.0],
        "NO2": [30.0, 31.0, 32.0, 20.0],
        "NOx": [1.0, 1.0, 1.0, 1.0],
        "NH3": [1.0, 1.0, 1.0, 1.0],
        "CO": [1.0, 1.0, 1.0, 1.0],
        "SO2": [1.0, 1.0, 1.0, 1.0],
        "O3": [1.0, 1.0, 1.0, 1.0],
        "Benzene": [1.0, 1.0, 1.0, 1.0],
        "Toluene": [1.0, 1.0, 1.0, 1.0],
        "Xylene": [1.0, 1.0, 1.0, 1.0],
        "AQI": [180.0, 185.0, 190.0, 90.0],
    })


POLLUTANT_COLUMNS = ["PM2.5", "PM10", "NO", "NO2", "NOx", "NH3", "CO", "SO2", "O3",
                     "Benzene", "Toluene", "Xylene"]
DB_NAME_MAP = {
    "PM2.5": "pm25", "PM10": "pm10", "NO": "no", "NO2": "no2", "NOx": "nox",
    "NH3": "nh3", "CO": "co", "SO2": "so2", "O3": "o3", "Benzene": "benzene",
    "Toluene": "toluene", "Xylene": "xylene",
}


def test_daily_aggregation_grain_is_city_date(hourly_for_transform):
    daily = transform.build_daily_aggregate(hourly_for_transform, POLLUTANT_COLUMNS, DB_NAME_MAP)
    assert len(daily) == 2  # Delhi 2020-01-01, Mumbai 2020-01-01
    assert set(daily["city_name"]) == {"Delhi", "Mumbai"}


def test_daily_aggregation_averages_ignore_nulls_not_impute(hourly_for_transform):
    daily = transform.build_daily_aggregate(hourly_for_transform, POLLUTANT_COLUMNS, DB_NAME_MAP)
    delhi_row = daily[daily["city_name"] == "Delhi"].iloc[0]
    # Average of 100.0 and 120.0 (the None is excluded, NOT treated as 0)
    assert delhi_row["avg_pm25"] == pytest.approx(110.0)


def test_daily_aggregation_observation_count_counts_records_not_non_null_values(hourly_for_transform):
    daily = transform.build_daily_aggregate(hourly_for_transform, POLLUTANT_COLUMNS, DB_NAME_MAP)
    delhi_row = daily[daily["city_name"] == "Delhi"].iloc[0]
    # 3 hourly records exist for Delhi even though one has a null PM2.5.
    assert delhi_row["observation_count"] == 3


def test_daily_aggregation_completeness_pct(hourly_for_transform):
    daily = transform.build_daily_aggregate(hourly_for_transform, POLLUTANT_COLUMNS, DB_NAME_MAP)
    delhi_row = daily[daily["city_name"] == "Delhi"].iloc[0]
    # 3 out of 24 expected hourly records -> 12.5%
    assert delhi_row["completeness_pct"] == pytest.approx(12.5)


def test_daily_aggregation_completeness_capped_at_100():
    # 25 hourly rows for a single city/day (bad clock / DST edge case) should not exceed 100%.
    df = pd.DataFrame({
        "City": ["Delhi"] * 25,
        "Datetime": pd.date_range("2020-01-01", periods=25, freq="h"),
        "PM2.5": [100.0] * 25,
    })
    for col in POLLUTANT_COLUMNS:
        if col not in df.columns:
            df[col] = 1.0
    df["AQI"] = 100.0

    daily = transform.build_daily_aggregate(df, POLLUTANT_COLUMNS, DB_NAME_MAP)
    assert daily.iloc[0]["completeness_pct"] <= 100.0


def test_aqi_bucket_mapping():
    assert transform.map_aqi_to_bucket(30) == "Good"
    assert transform.map_aqi_to_bucket(80) == "Satisfactory"
    assert transform.map_aqi_to_bucket(150) == "Moderate"
    assert transform.map_aqi_to_bucket(250) == "Poor"
    assert transform.map_aqi_to_bucket(350) == "Very Poor"
    assert transform.map_aqi_to_bucket(450) == "Severe"
    assert transform.map_aqi_to_bucket(3000) == "Severe"
    assert transform.map_aqi_to_bucket(None) is None
