"""
Reconciliation layer.

Compares the daily dataset we derive from city_hour.csv against the
independently published city_day.csv reference. This is a QA step, not
a second ETL source — city_day.csv is never loaded into the warehouse.

Because city_day.csv's own aggregation method is not documented (it may
use a different completeness rule, different rounding, or a stricter/looser
definition of a "valid" hourly reading), exact equality is not assumed.
Instead each metric is compared within a tolerance and both counts and a
match percentage are reported so a reader can judge materiality.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Relative tolerance used when comparing our derived averages to city_day.csv.
# Chosen to absorb minor floating point / rounding differences while still
# catching genuine aggregation discrepancies.
NUMERIC_RELATIVE_TOLERANCE = 0.05  # 5%


def _compare_numeric_metric(
    ours: pd.Series, theirs: pd.Series, metric_name: str, tolerance: float = NUMERIC_RELATIVE_TOLERANCE
) -> dict:
    """Compare two aligned numeric series within a relative tolerance."""
    both_present = ours.notna() & theirs.notna()
    compared = int(both_present.sum())

    if compared == 0:
        return dict(metric=metric_name, records_compared=0, matches=0, differences=0,
                     match_percentage=0.0, notes="No overlapping non-null records to compare.")

    o = ours[both_present]
    t = theirs[both_present]
    diff = (o - t).abs()
    denom = t.abs().replace(0, np.nan)
    rel_diff = (diff / denom).fillna(diff)  # if theirs is 0, fall back to absolute diff
    matches = int((rel_diff <= tolerance).sum())
    differences = compared - matches
    match_pct = round(matches / compared * 100, 2)

    notes = (
        f"Compared within a {tolerance*100:.0f}% relative tolerance to absorb rounding/"
        f"aggregation-method differences between the two sources."
    )
    return dict(metric=metric_name, records_compared=compared, matches=matches,
                differences=differences, match_percentage=match_pct, notes=notes)


def _compare_categorical_metric(ours: pd.Series, theirs: pd.Series, metric_name: str) -> dict:
    """Compare two aligned categorical series for exact match."""
    both_present = ours.notna() & theirs.notna()
    compared = int(both_present.sum())

    if compared == 0:
        return dict(metric=metric_name, records_compared=0, matches=0, differences=0,
                     match_percentage=0.0, notes="No overlapping non-null records to compare.")

    matches = int((ours[both_present] == theirs[both_present]).sum())
    differences = compared - matches
    match_pct = round(matches / compared * 100, 2)
    return dict(metric=metric_name, records_compared=compared, matches=matches,
                differences=differences, match_percentage=match_pct,
                notes="Exact string match required (categorical field).")


def reconcile_daily_against_city_day(
    derived_daily: pd.DataFrame, city_day: pd.DataFrame
) -> pd.DataFrame:
    """
    Compare our derived daily aggregate (from city_hour.csv) against the
    published city_day.csv on City + Date.

    Args:
        derived_daily: Output of transform.build_daily_aggregate(), with
            columns city_name, date, avg_<pollutant>..., source_aqi,
            source_aqi_bucket.
        city_day: Raw city_day.csv DataFrame with columns City, Date,
            <Pollutant>..., AQI, AQI_Bucket.

    Returns:
        Reconciliation report DataFrame with columns:
        metric, records_compared, matches, differences, match_percentage, notes.
    """
    ours = derived_daily.copy()
    theirs = city_day.copy()
    theirs["Date"] = pd.to_datetime(theirs["Date"], errors="coerce").dt.date
    ours["date"] = pd.to_datetime(ours["date"], errors="coerce").dt.date

    merged = ours.merge(
        theirs, left_on=["city_name", "date"], right_on=["City", "Date"],
        how="inner", suffixes=("_ours", "_theirs"),
    )
    logger.info("Reconciliation: %d overlapping City+Date rows between derived daily and city_day.csv", len(merged))

    metric_pairs = [
        ("avg_pm25", "PM2.5", "PM2.5"),
        ("avg_pm10", "PM10", "PM10"),
        ("avg_no2", "NO2", "NO2"),
        ("avg_nox", "NOx", "NOx"),
        ("avg_co", "CO", "CO"),
        ("avg_so2", "SO2", "SO2"),
        ("avg_o3", "O3", "O3"),
        ("source_aqi", "AQI", "AQI"),
    ]

    rows = [
        _compare_numeric_metric(merged[our_col], merged[their_col], display_name)
        for our_col, their_col, display_name in metric_pairs
    ]
    rows.append(_compare_categorical_metric(
        merged["source_aqi_bucket"], merged["AQI_Bucket"], "AQI_Bucket"
    ))

    report = pd.DataFrame(rows, columns=["metric", "records_compared", "matches", "differences",
                                          "match_percentage", "notes"])
    return report
