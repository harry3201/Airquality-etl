"""
Extraction layer.

Responsible only for reading raw source files into DataFrames.
No cleaning, validation, or business logic happens here.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def extract_csv(file_path: Path, parse_dates: list[str] | None = None) -> pd.DataFrame:
    """
    Read a CSV file into a DataFrame.

    Args:
        file_path: Path to the CSV file.
        parse_dates: Optional list of column names to attempt to parse as
            dates immediately. If omitted, date columns are left as
            strings so the validation layer can explicitly check them.

    Returns:
        The raw DataFrame, unmodified apart from optional date parsing.

    Raises:
        FileNotFoundError: If the source file does not exist.
    """
    if not file_path.exists():
        raise FileNotFoundError(
            f"Source file not found: {file_path}. "
            "Place the raw dataset under data/raw/ before running the pipeline."
        )

    logger.info("Extracting data from %s", file_path)
    df = pd.read_csv(file_path)

    if parse_dates:
        for col in parse_dates:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

    logger.info("Extracted %d rows, %d columns from %s", len(df), df.shape[1], file_path.name)
    return df


def extract_city_hour(file_path: Path) -> pd.DataFrame:
    """Extract the primary city_hour.csv source (grain: City + Datetime)."""
    return extract_csv(file_path)


def extract_city_day(file_path: Path) -> pd.DataFrame:
    """Extract the city_day.csv reference source (grain: City + Date), used only for reconciliation."""
    return extract_csv(file_path)
