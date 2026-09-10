-- Air Quality India ETL — Table creation
-- Run after 01_create_schema.sql.

SET search_path TO air_quality, public;

-- ---------------------------------------------------------------------------
-- Dimension: City
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_city (
    city_id     SERIAL PRIMARY KEY,
    city_name   VARCHAR(100) NOT NULL UNIQUE
);

-- ---------------------------------------------------------------------------
-- Fact: Hourly readings (grain: City + Datetime)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_air_quality_hourly (
    city_id     INTEGER      NOT NULL REFERENCES dim_city (city_id),
    datetime    TIMESTAMP    NOT NULL,
    pm25        NUMERIC(10, 2),
    pm10        NUMERIC(10, 2),
    no          NUMERIC(10, 2),
    no2         NUMERIC(10, 2),
    nox         NUMERIC(10, 2),
    nh3         NUMERIC(10, 2),
    co          NUMERIC(10, 2),
    so2         NUMERIC(10, 2),
    o3          NUMERIC(10, 2),
    benzene     NUMERIC(10, 2),
    toluene     NUMERIC(10, 2),
    xylene      NUMERIC(10, 2),
    aqi         NUMERIC(10, 2),
    aqi_bucket  VARCHAR(20),
    PRIMARY KEY (city_id, datetime)
);

CREATE INDEX IF NOT EXISTS idx_fact_hourly_datetime
    ON fact_air_quality_hourly (datetime);

CREATE INDEX IF NOT EXISTS idx_fact_hourly_city
    ON fact_air_quality_hourly (city_id);

-- ---------------------------------------------------------------------------
-- Fact: Daily aggregates (grain: City + Date), derived from the hourly fact
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_air_quality_daily (
    city_id             INTEGER      NOT NULL REFERENCES dim_city (city_id),
    date                DATE         NOT NULL,
    avg_pm25            NUMERIC(10, 2),
    avg_pm10            NUMERIC(10, 2),
    avg_no              NUMERIC(10, 2),
    avg_no2             NUMERIC(10, 2),
    avg_nox             NUMERIC(10, 2),
    avg_nh3             NUMERIC(10, 2),
    avg_co              NUMERIC(10, 2),
    avg_so2             NUMERIC(10, 2),
    avg_o3              NUMERIC(10, 2),
    avg_benzene         NUMERIC(10, 2),
    avg_toluene         NUMERIC(10, 2),
    avg_xylene          NUMERIC(10, 2),
    observation_count   INTEGER,
    completeness_pct    NUMERIC(5, 2),
    source_aqi          NUMERIC(10, 2),
    source_aqi_bucket   VARCHAR(20),
    PRIMARY KEY (city_id, date)
);

CREATE INDEX IF NOT EXISTS idx_fact_daily_date
    ON fact_air_quality_daily (date);

CREATE INDEX IF NOT EXISTS idx_fact_daily_city
    ON fact_air_quality_daily (city_id);

-- ---------------------------------------------------------------------------
-- ETL run log
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id          UUID PRIMARY KEY,
    pipeline_name   VARCHAR(100) NOT NULL,
    start_time      TIMESTAMP    NOT NULL,
    end_time        TIMESTAMP,
    source_rows     INTEGER,
    loaded_rows     INTEGER,
    status          VARCHAR(20),
    error_message   TEXT
);

CREATE INDEX IF NOT EXISTS idx_etl_run_log_start_time
    ON etl_run_log (start_time);
