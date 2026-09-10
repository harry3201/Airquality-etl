"""
Central configuration for the Air Quality ETL pipeline.

All paths and environment-driven settings live here so the rest of the
codebase never hardcodes a path or a credential.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Load variables from a .env file if one exists (never committed to git).
load_dotenv()

# ---------------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------------
BASE_DIR: Path = Path(__file__).resolve().parent.parent

DATA_DIR: Path = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
RAW_DATA_DIR: Path = DATA_DIR / "raw"
PROCESSED_DATA_DIR: Path = DATA_DIR / "processed"
REPORTS_DIR: Path = BASE_DIR / "reports"

CITY_HOUR_FILE: Path = RAW_DATA_DIR / "city_hour.csv"
CITY_DAY_FILE: Path = RAW_DATA_DIR / "city_day.csv"

DATA_QUALITY_REPORT_FILE: Path = REPORTS_DIR / "data_quality_report.csv"
RECONCILIATION_REPORT_FILE: Path = REPORTS_DIR / "reconciliation_report.csv"

# ---------------------------------------------------------------------------
# PostgreSQL connection settings (from environment variables only)
# ---------------------------------------------------------------------------
POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB: str = os.getenv("POSTGRES_DB", "air_quality")
POSTGRES_USER: str = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "")

# ---------------------------------------------------------------------------
# Domain constants
# ---------------------------------------------------------------------------
POLLUTANT_COLUMNS: list[str] = [
    "PM2.5", "PM10", "NO", "NO2", "NOx", "NH3",
    "CO", "SO2", "O3", "Benzene", "Toluene", "Xylene",
]

# Column -> database-friendly name (used consistently across transform/load/sql)
POLLUTANT_DB_NAMES: dict[str, str] = {
    "PM2.5": "pm25",
    "PM10": "pm10",
    "NO": "no",
    "NO2": "no2",
    "NOx": "nox",
    "NH3": "nh3",
    "CO": "co",
    "SO2": "so2",
    "O3": "o3",
    "Benzene": "benzene",
    "Toluene": "toluene",
    "Xylene": "xylene",
}

REQUIRED_HOUR_COLUMNS: list[str] = (
    ["City", "Datetime"] + POLLUTANT_COLUMNS + ["AQI", "AQI_Bucket"]
)
REQUIRED_DAY_COLUMNS: list[str] = (
    ["City", "Date"] + POLLUTANT_COLUMNS + ["AQI", "AQI_Bucket"]
)

# Expected number of hourly readings per city per calendar day.
EXPECTED_HOURLY_READINGS_PER_DAY: int = 24

# Thresholds used to flag (never silently drop) suspicious pollutant readings.
# Derived from domain knowledge + observed 99.9th percentiles during profiling.
# These are "investigate" thresholds, not deletion thresholds.
SUSPICIOUS_VALUE_THRESHOLDS: dict[str, float] = {
    "PM2.5": 1000.0,
    "PM10": 1000.0,
    "NO": 400.0,
    "NO2": 400.0,
    "NOx": 400.0,
    "NH3": 400.0,
    "CO": 200.0,
    "SO2": 200.0,
    "O3": 250.0,
    "Benzene": 450.0,
    "Toluene": 450.0,
    "Xylene": 100.0,
    "AQI": 500.0,  # official Indian AQI scale tops out at 500
}
