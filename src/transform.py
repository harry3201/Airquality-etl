"""
Transformation layer.

Builds the daily analytical dataset (City + Date grain) from the validated
hourly dataset (City + Datetime grain). This module intentionally does not
touch PostgreSQL — it only produces clean, well-typed pandas DataFrames
that the load layer will persist.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Standard CPCB (Central Pollution Control Board) AQI bucket breakpoints.
# Used ONLY to label an already-averaged AQI value for convenience — this
# is NOT a re-derivation of AQI from pollutant sub-indices, and it is
# clearly distinct from the official city_day.csv AQI_Bucket, against
# which it is reconciled downstream.
_AQI_BUCKET_BREAKPOINTS: list[tuple[float, str]] = [
    (50, "Good"),
    (100, "Satisfactory"),
    (200, "Moderate"),
    (300, "Poor"),
    (400, "Very Poor"),
    (float("inf"), "Severe"),
]


def map_aqi_to_bucket(aqi: float | None) -> str | None:
    """Map a numeric AQI value to its CPCB category label."""
    if aqi is None or (isinstance(aqi, float) and np.isnan(aqi)):
        return None
    for upper_bound, label in _AQI_BUCKET_BREAKPOINTS:
        if aqi <= upper_bound:
            return label
    return "Severe"


def prepare_hourly_for_load(
    df: pd.DataFrame,
    pollutant_columns: list[str],
    db_name_map: dict[str, str],
    city_col: str = "City",
    datetime_col: str = "Datetime",
) -> pd.DataFrame:
    """
    Rename/select columns on the hourly DataFrame to match the
    fact_air_quality_hourly database schema. Values are passed through
    unmodified — no imputation, no deletion of extreme values.
    """
    working = df.copy()
    working[datetime_col] = pd.to_datetime(working[datetime_col], errors="coerce")

    rename_map = {city_col: "city_name", datetime_col: "datetime", **db_name_map}
    rename_map["AQI"] = "aqi"
    rename_map["AQI_Bucket"] = "aqi_bucket"

    keep_cols = [city_col, datetime_col] + pollutant_columns + ["AQI", "AQI_Bucket"]
    working = working[keep_cols].rename(columns=rename_map)
    return working


def build_daily_aggregate(
    df: pd.DataFrame,
    pollutant_columns: list[str],
    db_name_map: dict[str, str],
    city_col: str = "City",
    datetime_col: str = "Datetime",
    expected_hours_per_day: int = 24,
) -> pd.DataFrame:
    """
    Aggregate the hourly dataset to a daily City + Date grain.

    For each pollutant, computes the mean of available (non-null) hourly
    readings — a NULL pollutant value is simply excluded from that day's
    average, it is never treated as zero and never imputed.

    observation_count = number of hourly *records* present for that
    City + Date (regardless of whether their pollutant values are null).
    completeness_pct = observation_count / expected_hours_per_day * 100,
    capped at 100.

    Returns:
        DataFrame at City + Date grain with columns:
        city_name, date, avg_<pollutant>..., observation_count,
        completeness_pct, source_aqi, source_aqi_bucket
    """
    working = df.copy()
    working[datetime_col] = pd.to_datetime(working[datetime_col], errors="coerce")
    working = working.dropna(subset=[datetime_col, city_col])
    working["date"] = working[datetime_col].dt.date

    group_cols = [city_col, "date"]

    # Mean of available pollutant readings (skipna is pandas default for mean).
    agg_dict = {col: "mean" for col in pollutant_columns}
    agg_dict["AQI"] = "mean"
    daily = working.groupby(group_cols).agg(agg_dict)

    # observation_count = count of hourly RECORDS present, independent of
    # whether individual pollutant values within them are null.
    observation_count = working.groupby(group_cols).size()
    daily["observation_count"] = observation_count

    daily["completeness_pct"] = (
        daily["observation_count"] / expected_hours_per_day * 100
    ).clip(upper=100).round(2)

    daily = daily.reset_index()

    daily["source_aqi"] = daily["AQI"].round(2)
    daily["source_aqi_bucket"] = daily["source_aqi"].apply(map_aqi_to_bucket)
    daily = daily.drop(columns=["AQI"])

    rename_map = {city_col: "city_name", **{k: f"avg_{v}" for k, v in db_name_map.items()}}
    daily = daily.rename(columns=rename_map)

    logger.info("Built daily aggregate: %d City+Date rows from %d hourly rows",
                len(daily), len(working))
    return daily
