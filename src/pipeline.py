"""
Pipeline entry point.

Orchestrates: extract -> validate -> transform -> load -> reconcile,
and writes the data-quality and reconciliation reports.

Run with:
    python -m src.pipeline
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime

import pandas as pd

from src import config, extract, validate, transform, load, reconcile

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
logger = logging.getLogger("pipeline")

PIPELINE_NAME = "air_quality_etl"


def run_pipeline(load_to_postgres: bool = True) -> dict:
    """
    Execute the full ETL pipeline and return a summary dict.

    Args:
        load_to_postgres: If False, skips the PostgreSQL load step (useful
            for environments where a database isn't available) but still
            runs extraction, validation, transformation, and reconciliation,
            and still writes the CSV reports.
    """
    summary: dict = {"pipeline_status": "STARTED"}

    # -- 1. Extract ---------------------------------------------------------
    hour_df = extract.extract_city_hour(config.CITY_HOUR_FILE)
    day_df = extract.extract_city_day(config.CITY_DAY_FILE)
    summary["source_rows"] = len(hour_df)

    # -- 2. Validate ----------------------------------------------------------
    dq_report = validate.run_full_validation(
        hour_df,
        required_columns=config.REQUIRED_HOUR_COLUMNS,
        pollutant_columns=config.POLLUTANT_COLUMNS,
        suspicious_thresholds=config.SUSPICIOUS_VALUE_THRESHOLDS,
        datetime_col="Datetime",
        city_col="City",
    )
    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    dq_report.to_csv(config.DATA_QUALITY_REPORT_FILE, index=False)
    logger.info("Data quality report written to %s", config.DATA_QUALITY_REPORT_FILE)

    valid_rows, invalid_rows = validate.get_structurally_valid_rows(hour_df)
    summary["validated_rows"] = len(valid_rows)
    summary["invalid_rows"] = len(invalid_rows)
    summary["cities"] = valid_rows["City"].nunique()

    # -- 3. Transform ---------------------------------------------------------
    hourly_for_load = transform.prepare_hourly_for_load(
        valid_rows, config.POLLUTANT_COLUMNS, config.POLLUTANT_DB_NAMES
    )
    daily_agg = transform.build_daily_aggregate(
        valid_rows, config.POLLUTANT_COLUMNS, config.POLLUTANT_DB_NAMES
    )
    summary["daily_rows_generated"] = len(daily_agg)

    processed_dir = config.PROCESSED_DATA_DIR
    processed_dir.mkdir(parents=True, exist_ok=True)
    daily_agg.to_csv(processed_dir / "daily_aggregate.csv", index=False)

    # -- 4/5/6/7/8. Load to PostgreSQL -----------------------------------------
    summary["hourly_rows_loaded"] = 0
    summary["daily_rows_loaded"] = 0

    if load_to_postgres:
        conn = None
        run_id = None
        try:
            conn = load.get_connection()
            run_id = load.start_etl_run(conn, PIPELINE_NAME)

            city_id_map = load.load_dim_city(conn, valid_rows["City"].unique().tolist())
            hourly_loaded = load.load_fact_hourly(conn, hourly_for_load, city_id_map)
            daily_loaded = load.load_fact_daily(conn, daily_agg, city_id_map)

            summary["hourly_rows_loaded"] = hourly_loaded
            summary["daily_rows_loaded"] = daily_loaded

            load.finish_etl_run(
                conn, run_id,
                source_rows=summary["source_rows"],
                loaded_rows=hourly_loaded,
                status="SUCCESS",
            )
            summary["pipeline_status"] = "SUCCESS"
        except Exception as exc:  # noqa: BLE001 - top-level pipeline guard
            logger.exception("Pipeline failed during load step")
            summary["pipeline_status"] = "FAILED"
            summary["error_message"] = str(exc)
            if conn is not None and run_id is not None:
                try:
                    load.finish_etl_run(
                        conn, run_id,
                        source_rows=summary["source_rows"],
                        loaded_rows=summary["hourly_rows_loaded"],
                        status="FAILED",
                        error_message=str(exc),
                    )
                except Exception:
                    logger.exception("Could not record failure in etl_run_log")
        finally:
            if conn is not None:
                conn.close()
    else:
        logger.warning("load_to_postgres=False: skipping PostgreSQL load step")
        summary["pipeline_status"] = "SUCCESS_NO_DB_LOAD"

    # -- 9. Reconciliation ------------------------------------------------------
    recon_report = reconcile.reconcile_daily_against_city_day(daily_agg, day_df)
    recon_report.to_csv(config.RECONCILIATION_REPORT_FILE, index=False)
    logger.info("Reconciliation report written to %s", config.RECONCILIATION_REPORT_FILE)

    if len(recon_report):
        summary["reconciliation_match_rate"] = round(recon_report["match_percentage"].mean(), 2)
    else:
        summary["reconciliation_match_rate"] = None

    # -- 10. Print summary --------------------------------------------------
    _print_summary(summary)
    return summary


def _print_summary(summary: dict) -> None:
    print("\n" + "=" * 60)
    print("AIR QUALITY ETL — EXECUTION SUMMARY")
    print("=" * 60)
    print(f"Source rows:                {summary.get('source_rows')}")
    print(f"Validated rows:              {summary.get('validated_rows')}")
    print(f"Invalid rows:                {summary.get('invalid_rows')}")
    print(f"Cities:                      {summary.get('cities')}")
    print(f"Daily rows generated:        {summary.get('daily_rows_generated')}")
    print(f"Hourly rows loaded:          {summary.get('hourly_rows_loaded')}")
    print(f"Daily rows loaded:           {summary.get('daily_rows_loaded')}")
    print(f"Reconciliation match rate:   {summary.get('reconciliation_match_rate')}%")
    print(f"Pipeline status:             {summary.get('pipeline_status')}")
    if summary.get("error_message"):
        print(f"Error:                       {summary['error_message']}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    load_to_db = "--no-db" not in sys.argv
    run_pipeline(load_to_postgres=load_to_db)
