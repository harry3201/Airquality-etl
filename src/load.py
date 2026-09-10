"""
Load layer.

Handles all interaction with PostgreSQL: connecting, upserting dimension
and fact rows, and recording ETL run metadata. Every load is idempotent —
running the pipeline twice must not create duplicate rows — via
INSERT ... ON CONFLICT ... DO UPDATE.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any

import pandas as pd
import psycopg2
import psycopg2.extras

from src import config

logger = logging.getLogger(__name__)


def get_connection() -> "psycopg2.extensions.connection":
    """Open a PostgreSQL connection using environment-configured credentials."""
    return psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        # All project tables live in the air_quality schema (see sql/01_create_schema.sql).
        options="-c search_path=air_quality,public",
    )


def load_dim_city(conn, cities: list[str]) -> dict[str, int]:
    """
    Upsert distinct city names into dim_city and return a
    {city_name: city_id} mapping for use by fact loaders.
    """
    cities = sorted(set(c for c in cities if pd.notna(c)))
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO dim_city (city_name)
            VALUES %s
            ON CONFLICT (city_name) DO NOTHING
            """,
            [(c,) for c in cities],
        )
        cur.execute("SELECT city_id, city_name FROM dim_city")
        mapping = {name: city_id for city_id, name in cur.fetchall()}
    conn.commit()
    logger.info("Loaded/verified %d cities in dim_city", len(mapping))
    return mapping


def _df_to_records(df: pd.DataFrame, columns: list[str]) -> list[tuple]:
    """Convert a DataFrame to a list of tuples, replacing NaN with None."""
    subset = df[columns].astype(object).where(pd.notnull(df[columns]), None)
    return list(subset.itertuples(index=False, name=None))


def load_fact_hourly(conn, df: pd.DataFrame, city_id_map: dict[str, int], batch_size: int = 10_000) -> int:
    """
    Upsert rows into fact_air_quality_hourly, keyed on (city_id, datetime).
    Idempotent: rerunning updates existing rows in place rather than duplicating.
    """
    working = df.copy()
    working["city_id"] = working["city_name"].map(city_id_map)
    working = working.dropna(subset=["city_id"])
    working["city_id"] = working["city_id"].astype(int)

    columns = [
        "city_id", "datetime", "pm25", "pm10", "no", "no2", "nox", "nh3",
        "co", "so2", "o3", "benzene", "toluene", "xylene", "aqi", "aqi_bucket",
    ]
    records = _df_to_records(working, columns)

    sql = f"""
        INSERT INTO fact_air_quality_hourly ({", ".join(columns)})
        VALUES %s
        ON CONFLICT (city_id, datetime) DO UPDATE SET
            {", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c not in ("city_id", "datetime"))}
    """

    total_loaded = 0
    with conn.cursor() as cur:
        for start in range(0, len(records), batch_size):
            batch = records[start:start + batch_size]
            psycopg2.extras.execute_values(cur, sql, batch, page_size=batch_size)
            total_loaded += len(batch)
    conn.commit()
    logger.info("Upserted %d rows into fact_air_quality_hourly", total_loaded)
    return total_loaded


def load_fact_daily(conn, df: pd.DataFrame, city_id_map: dict[str, int]) -> int:
    """
    Upsert rows into fact_air_quality_daily, keyed on (city_id, date).
    Idempotent: rerunning updates existing rows in place rather than duplicating.
    """
    working = df.copy()
    working["city_id"] = working["city_name"].map(city_id_map)
    working = working.dropna(subset=["city_id"])
    working["city_id"] = working["city_id"].astype(int)

    columns = [
        "city_id", "date", "avg_pm25", "avg_pm10", "avg_no", "avg_no2", "avg_nox",
        "avg_nh3", "avg_co", "avg_so2", "avg_o3", "avg_benzene", "avg_toluene",
        "avg_xylene", "observation_count", "completeness_pct",
        "source_aqi", "source_aqi_bucket",
    ]
    records = _df_to_records(working, columns)

    sql = f"""
        INSERT INTO fact_air_quality_daily ({", ".join(columns)})
        VALUES %s
        ON CONFLICT (city_id, date) DO UPDATE SET
            {", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c not in ("city_id", "date"))}
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, records, page_size=5_000)
    conn.commit()
    logger.info("Upserted %d rows into fact_air_quality_daily", len(records))
    return len(records)


def start_etl_run(conn, pipeline_name: str) -> str:
    """Insert a new etl_run_log row with status RUNNING and return its run_id."""
    run_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_run_log (run_id, pipeline_name, start_time, status)
            VALUES (%s, %s, %s, %s)
            """,
            (run_id, pipeline_name, datetime.utcnow(), "RUNNING"),
        )
    conn.commit()
    return run_id


def finish_etl_run(
    conn,
    run_id: str,
    source_rows: int,
    loaded_rows: int,
    status: str,
    error_message: str | None = None,
) -> None:
    """Update the etl_run_log row for run_id with final status and counts."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE etl_run_log
            SET end_time = %s, source_rows = %s, loaded_rows = %s,
                status = %s, error_message = %s
            WHERE run_id = %s
            """,
            (datetime.utcnow(), source_rows, loaded_rows, status, error_message, run_id),
        )
    conn.commit()
