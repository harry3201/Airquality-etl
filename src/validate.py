"""
Validation layer.

Provides small, reusable, single-purpose validation functions plus an
orchestrator that assembles them into a data-quality report.

Design principles enforced here:
    * Extreme/suspicious numeric values are FLAGGED, never silently deleted.
    * Missing pollutant measurements are NEVER imputed.
    * A missing hourly *record* (timestamp gap) is treated differently from
      a record that exists but has NULL pollutant readings.
    * Rows are only ever dropped from the pipeline when they are structurally
      broken (no City, unparseable Datetime, or an exact duplicate key) —
      never because a value looked unusual.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

ReportRow = dict[str, Any]


def _row(check_name: str, metric: str, value: Any, status: str, description: str) -> ReportRow:
    return {
        "check_name": check_name,
        "metric": metric,
        "value": value,
        "status": status,
        "description": description,
    }


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def check_required_columns(df: pd.DataFrame, required_columns: list[str]) -> list[ReportRow]:
    """Verify every required column is present in the DataFrame."""
    missing = [c for c in required_columns if c not in df.columns]
    status = "FAIL" if missing else "PASS"
    return [_row(
        "required_columns", "missing_columns", ", ".join(missing) if missing else "none",
        status, "Checks that all columns required by the schema are present in the source file.",
    )]


def check_data_types(df: pd.DataFrame, numeric_columns: list[str]) -> list[ReportRow]:
    """Verify pollutant columns are numeric (or coercible to numeric)."""
    rows: list[ReportRow] = []
    for col in numeric_columns:
        if col not in df.columns:
            continue
        is_numeric = pd.api.types.is_numeric_dtype(df[col])
        coerced = pd.to_numeric(df[col], errors="coerce")
        non_coercible = int((coerced.isna() & df[col].notna()).sum())
        status = "PASS" if is_numeric or non_coercible == 0 else "WARN"
        rows.append(_row(
            "data_types", f"{col}_non_numeric_values", non_coercible,
            status, f"Count of values in '{col}' that cannot be parsed as numeric.",
        ))
    return rows


def check_null_city(df: pd.DataFrame, city_col: str = "City") -> list[ReportRow]:
    """Count rows with a missing City value; City is a required key column."""
    null_count = int(df[city_col].isna().sum())
    status = "PASS" if null_count == 0 else "FAIL"
    return [_row(
        "null_city", "null_city_rows", null_count, status,
        "Rows missing a City value cannot be attributed to a dimension and are excluded from load.",
    )]


def check_invalid_datetime(df: pd.DataFrame, datetime_col: str) -> list[ReportRow]:
    """Count rows whose datetime/date value cannot be parsed."""
    parsed = pd.to_datetime(df[datetime_col], errors="coerce")
    invalid_count = int((parsed.isna() & df[datetime_col].notna()).sum())
    null_count = int(df[datetime_col].isna().sum())
    total_bad = invalid_count + null_count
    status = "PASS" if total_bad == 0 else "FAIL"
    return [_row(
        "invalid_datetime", f"unparseable_or_missing_{datetime_col.lower()}", total_bad,
        status, f"Rows where '{datetime_col}' is missing or cannot be parsed as a timestamp.",
    )]


def check_duplicates(df: pd.DataFrame, subset: list[str]) -> list[ReportRow]:
    """Count exact duplicate rows on the grain key (e.g. City + Datetime)."""
    dup_count = int(df.duplicated(subset=subset, keep="first").sum())
    status = "PASS" if dup_count == 0 else "WARN"
    return [_row(
        "duplicate_keys", f"duplicate_{'+'.join(subset)}", dup_count, status,
        f"Rows sharing the same {' + '.join(subset)} key, which violates the expected grain.",
    )]


def check_negative_values(df: pd.DataFrame, pollutant_columns: list[str]) -> list[ReportRow]:
    """Count negative readings per pollutant. Flagged for review, not deleted."""
    rows: list[ReportRow] = []
    for col in pollutant_columns:
        if col not in df.columns:
            continue
        neg_count = int((df[col] < 0).sum())
        status = "PASS" if neg_count == 0 else "WARN"
        rows.append(_row(
            "negative_values", f"{col}_negative_count", neg_count, status,
            f"Physically impossible negative readings in '{col}'. Flagged for investigation, not deleted.",
        ))
    return rows


def flag_suspicious_values(
    df: pd.DataFrame, thresholds: dict[str, float]
) -> pd.DataFrame:
    """
    Add boolean '<col>_flag_suspicious' columns for values exceeding thresholds.

    Values are never modified or removed — only flagged so downstream
    consumers (reports, analysts) can investigate.
    """
    flagged = df.copy()
    for col, threshold in thresholds.items():
        if col in flagged.columns:
            flagged[f"{col}_flag_suspicious"] = flagged[col] > threshold
    return flagged


def check_suspicious_values(df: pd.DataFrame, thresholds: dict[str, float]) -> list[ReportRow]:
    """Count values exceeding domain-informed 'suspicious' thresholds per column."""
    rows: list[ReportRow] = []
    for col, threshold in thresholds.items():
        if col not in df.columns:
            continue
        count = int((df[col] > threshold).sum())
        status = "PASS" if count == 0 else "WARN"
        rows.append(_row(
            "suspicious_values", f"{col}_above_{threshold}", count, status,
            f"Readings in '{col}' above {threshold}, an unusually high value. "
            "Flagged for investigation; not automatically removed.",
        ))
    return rows


def check_missing_percentages(df: pd.DataFrame, columns: list[str]) -> list[ReportRow]:
    """Report the percentage of missing values per column."""
    rows: list[ReportRow] = []
    total = len(df)
    for col in columns:
        if col not in df.columns:
            continue
        missing_pct = round(float(df[col].isna().mean() * 100), 2) if total else 0.0
        status = "WARN" if missing_pct > 30 else "PASS"
        rows.append(_row(
            "missing_percentage", f"{col}_missing_pct", missing_pct, status,
            f"Percentage of rows where '{col}' is NULL. Missing measurements are never imputed.",
        ))
    return rows


def check_city_coverage(df: pd.DataFrame, city_col: str = "City") -> list[ReportRow]:
    """Report the number and list of distinct cities present in the source."""
    cities = sorted(df[city_col].dropna().unique().tolist())
    return [_row(
        "city_coverage", "distinct_city_count", len(cities), "INFO",
        f"Distinct cities present in the source: {', '.join(cities)}",
    )]


def check_timestamp_gaps(
    df: pd.DataFrame, city_col: str = "City", datetime_col: str = "Datetime"
) -> list[ReportRow]:
    """
    For each city, compare the number of actual hourly records against the
    number of hours implied by its own min/max datetime range. A gap here
    means an hourly *record is entirely absent* — this is distinct from a
    record that exists but has NULL pollutant values.
    """
    working = df.copy()
    working[datetime_col] = pd.to_datetime(working[datetime_col], errors="coerce")
    working = working.dropna(subset=[datetime_col])

    grouped = working.groupby(city_col)[datetime_col].agg(["min", "max", "count"])
    grouped["expected_hours"] = (
        (grouped["max"] - grouped["min"]).dt.total_seconds() / 3600 + 1
    ).astype(int)
    grouped["missing_hours"] = grouped["expected_hours"] - grouped["count"]
    grouped["gap_pct"] = (grouped["missing_hours"] / grouped["expected_hours"] * 100).round(2)

    total_missing_hours = int(grouped["missing_hours"].sum())
    total_expected_hours = int(grouped["expected_hours"].sum())
    overall_gap_pct = round(total_missing_hours / total_expected_hours * 100, 2) if total_expected_hours else 0.0

    status = "PASS" if overall_gap_pct == 0 else "WARN"
    worst_city = grouped["gap_pct"].idxmax() if len(grouped) else "n/a"
    worst_pct = float(grouped["gap_pct"].max()) if len(grouped) else 0.0

    return [
        _row(
            "timestamp_gaps", "overall_missing_hourly_records", total_missing_hours, status,
            "Total hourly records entirely absent from the source, within each city's own active date range "
            "(a gap = a missing row, not a NULL reading).",
        ),
        _row(
            "timestamp_gaps", "overall_gap_pct", overall_gap_pct, status,
            "Percentage of expected hourly records that are missing across all cities.",
        ),
        _row(
            "timestamp_gaps", "worst_city_gap", f"{worst_city} ({worst_pct}%)", "INFO",
            "City with the highest proportion of missing hourly records.",
        ),
    ]


def check_daily_temporal_completeness(
    df: pd.DataFrame,
    city_col: str = "City",
    datetime_col: str = "Datetime",
    expected_hours_per_day: int = 24,
) -> list[ReportRow]:
    """
    Report what fraction of City+Date groups have the full expected number
    of hourly records (24). This is a dataset-level summary; the per-row
    completeness_pct is computed in the transform layer's daily aggregation.
    """
    working = df.copy()
    working[datetime_col] = pd.to_datetime(working[datetime_col], errors="coerce")
    working = working.dropna(subset=[datetime_col])
    working["_date"] = working[datetime_col].dt.date

    counts = working.groupby([city_col, "_date"]).size()
    total_groups = len(counts)
    complete_groups = int((counts >= expected_hours_per_day).sum())
    complete_pct = round(complete_groups / total_groups * 100, 2) if total_groups else 0.0

    status = "PASS" if complete_pct > 80 else "WARN"
    return [_row(
        "daily_temporal_completeness", "city_days_with_full_24_hours_pct", complete_pct, status,
        f"Percentage of City+Date groups containing all {expected_hours_per_day} expected hourly records.",
    )]


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_full_validation(
    df: pd.DataFrame,
    required_columns: list[str],
    pollutant_columns: list[str],
    suspicious_thresholds: dict[str, float],
    datetime_col: str = "Datetime",
    city_col: str = "City",
    key_columns: list[str] | None = None,
) -> pd.DataFrame:
    """
    Run the full suite of validation checks and return a single
    data-quality report DataFrame with columns:
    check_name, metric, value, status, description.
    """
    key_columns = key_columns or [city_col, datetime_col]
    rows: list[ReportRow] = []

    rows += check_required_columns(df, required_columns)
    rows += check_data_types(df, pollutant_columns + ["AQI"])
    rows += check_null_city(df, city_col)
    rows += check_invalid_datetime(df, datetime_col)
    rows += check_duplicates(df, key_columns)
    rows += check_negative_values(df, pollutant_columns)
    rows += check_suspicious_values(df, suspicious_thresholds)
    rows += check_missing_percentages(df, pollutant_columns + ["AQI"])
    rows += check_city_coverage(df, city_col)

    if datetime_col == "Datetime":
        rows += check_timestamp_gaps(df, city_col, datetime_col)
        rows += check_daily_temporal_completeness(df, city_col, datetime_col)

    report = pd.DataFrame(rows, columns=["check_name", "metric", "value", "status", "description"])
    logger.info("Validation complete: %d checks run, %d FAIL, %d WARN",
                len(report), (report["status"] == "FAIL").sum(), (report["status"] == "WARN").sum())
    return report


def get_structurally_valid_rows(
    df: pd.DataFrame, city_col: str = "City", datetime_col: str = "Datetime"
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split the DataFrame into structurally valid vs invalid rows.

    A row is only considered *structurally invalid* (and therefore excluded
    from downstream loading) if it has a null City, an unparseable/missing
    Datetime, or is an exact duplicate of the City+Datetime key. Rows with
    unusual/extreme pollutant values remain valid and are only flagged.

    Returns:
        (valid_rows, invalid_rows)
    """
    working = df.copy()
    working["_parsed_dt"] = pd.to_datetime(working[datetime_col], errors="coerce")

    invalid_mask = working[city_col].isna() | working["_parsed_dt"].isna()
    duplicate_mask = working.duplicated(subset=[city_col, datetime_col], keep="first")

    drop_mask = invalid_mask | duplicate_mask
    valid_rows = working.loc[~drop_mask].drop(columns=["_parsed_dt"]).copy()
    invalid_rows = working.loc[drop_mask].drop(columns=["_parsed_dt"]).copy()

    valid_rows[datetime_col] = pd.to_datetime(valid_rows[datetime_col], errors="coerce")

    logger.info("Structural validation: %d valid rows, %d invalid rows excluded",
                len(valid_rows), len(invalid_rows))
    return valid_rows, invalid_rows
