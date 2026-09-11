# Air Quality India — ETL & Analytics Pipeline

A portfolio data-engineering project that builds a complete ETL
pipeline on top of India's public hourly air-quality dataset: extraction,
validation, transformation, idempotent PostgreSQL loading, cross-source
reconciliation, and SQL analytics.

## 1. Project Objective

Take a large (~708K row), messy, real-world hourly air-quality dataset and
turn it into a reliable, queryable warehouse — while being explicit and
honest about data quality rather than hiding it behind silent deletion or
imputation. The project demonstrates core data-engineering fundamentals:
extract/validate/transform/load separation, idempotent loading, SQL schema
design, reconciliation between two independently-published sources, and
automated testing — using nothing but Python, Pandas, PostgreSQL, and SQL.

## 2. Dataset

- **`city_hour.csv`** (primary source) — hourly pollutant readings per city.
  707,875 rows × 16 columns, 26 cities, Jan 2015 – Jul 2020.
- **`city_day.csv`** (reconciliation reference) — daily pollutant readings
  per city, published independently by the same source. 29,531 rows × 16
  columns, same 26 cities and date range.

Columns in both: `City`, `Datetime`/`Date`, `PM2.5`, `PM10`, `NO`, `NO2`,
`NOx`, `NH3`, `CO`, `SO2`, `O3`, `Benzene`, `Toluene`, `Xylene`, `AQI`,
`AQI_Bucket`.

`city_day.csv` is **not** treated as a second independent source for the
main ETL — it is used only to sanity-check the daily aggregate this pipeline
derives from `city_hour.csv` (see §9, Reconciliation).

## 3. Architecture

```
city_hour.csv
    │
    ▼
 extract  ──►  validate  ──►  transform  ──►  PostgreSQL load
                                                    │
                                                    ▼
                                   reconciliation against city_day.csv
                                                    │
                                                    ▼
                                            SQL analytics / reports
```

Design principle: the Jupyter notebook (`notebooks/01_data_profiling.ipynb`)
is for exploratory profiling only. All reusable ETL logic lives in `src/`
modules and is unit-tested independently of the notebook.

## 4. Data Flow

1. **Extract** (`src/extract.py`) — reads the raw CSVs, no transformation.
2. **Validate** (`src/validate.py`) — runs 11 reusable checks and produces
   `reports/data_quality_report.csv`. Only *structurally* broken rows (null
   City, unparseable Datetime, duplicate key) are excluded from load —
   extreme values are flagged, never deleted; missing values are never
   imputed.
3. **Transform** (`src/transform.py`) — aggregates the validated hourly data
   to a City+Date daily grain, computing `observation_count` and
   `completeness_pct` alongside pollutant averages.
4. **Load** (`src/load.py`) — idempotent upserts into PostgreSQL
   (`dim_city`, `fact_air_quality_hourly`, `fact_air_quality_daily`,
   `etl_run_log`).
5. **Reconcile** (`src/reconcile.py`) — compares the derived daily aggregate
   against `city_day.csv`, producing `reports/reconciliation_report.csv`.
6. **Analyze** (`sql/03_analytics.sql`) — ~10 analytical queries against the
   loaded warehouse.

All of this is orchestrated by `src/pipeline.py`.

## 5. Data Quality Challenges

Full findings: [`docs/data_quality.md`](docs/data_quality.md). Highlights
from actually running validation against the full 707,875-row dataset:

- **Structurally clean**: 0 null Citys, 0 invalid Datetimes, 0 duplicate
  City+Datetime keys, 0 negative pollutant values, 0 timestamp gaps within
  each city's active window.
- **Substantial missingness at the measurement level**: from 12% (CO) to
  64% (Xylene) of readings are NULL. These are real sensor-availability
  gaps, not data-entry errors, and are never imputed.
- **Extreme values exist and are flagged, not deleted**: e.g. 11,135 AQI
  readings exceed the official 500-point scale ceiling. These may reflect
  genuine severe pollution events (festival fireworks, crop burning) or
  sensor faults — the pipeline surfaces them for investigation rather than
  making that call automatically.
- **Timestamp gaps vs. NULL measurements are tracked as distinct concepts**
  (see `check_timestamp_gaps` vs `check_missing_percentages` in
  `src/validate.py`) — a missing *record* is not the same problem as a
  present record with a NULL value.

## 6. Transformation Logic

`transform.build_daily_aggregate()` groups validated hourly rows by
City+Date and computes, per pollutant, the mean of **available** (non-null)
readings — a NULL value is excluded from that day's average, never treated
as zero. Alongside each average:

- `observation_count` — number of hourly *records* present that day
  (independent of whether their pollutant values are null).
- `completeness_pct` — `observation_count / 24 × 100`, capped at 100.
- `source_aqi` — mean of the hourly `AQI` values already computed by the
  source (not a re-derivation from pollutant sub-indices).
- `source_aqi_bucket` — the standard CPCB category label
  (Good/Satisfactory/Moderate/Poor/Very Poor/Severe) applied to `source_aqi`,
  clearly distinct from `city_day.csv`'s own official `AQI_Bucket`, which it
  is reconciled against.

## 7. Database Schema

```
dim_city
  city_id     SERIAL PK
  city_name   VARCHAR UNIQUE

fact_air_quality_hourly            (grain: city_id + datetime)
  city_id     FK → dim_city
  datetime
  pm25, pm10, no, no2, nox, nh3, co, so2, o3, benzene, toluene, xylene
  aqi, aqi_bucket
  PK (city_id, datetime)

fact_air_quality_daily             (grain: city_id + date)
  city_id     FK → dim_city
  date
  avg_pm25 ... avg_xylene
  observation_count, completeness_pct
  source_aqi, source_aqi_bucket
  PK (city_id, date)

etl_run_log
  run_id UUID PK, pipeline_name, start_time, end_time,
  source_rows, loaded_rows, status, error_message
```

Full DDL, indexes, and FKs: `sql/01_create_schema.sql`, `sql/02_create_tables.sql`.

## 8. ETL Process

Run end-to-end with:

```bash
python -m src.pipeline
```

or, without a PostgreSQL connection available (still runs extract → validate
→ transform → reconcile, and still writes both CSV reports):

```bash
python -m src.pipeline --no-db
```

The pipeline is **idempotent** — running it twice does not create duplicate
rows, because loading uses `INSERT ... ON CONFLICT (city_id, datetime|date)
DO UPDATE` rather than delete-and-reload.

## 9. Reconciliation

After building the daily aggregate from `city_hour.csv`, it is compared
against the independently published `city_day.csv` on City+Date, for PM2.5,
PM10, NO2, NOx, CO, SO2, O3, AQI, and AQI_Bucket. Because the two sources'
exact aggregation methodology isn't documented, numeric metrics are compared
within a 5% relative tolerance rather than requiring exact equality — see
`src/reconcile.py`.

**Actual result on the full dataset:** ~99.8% average match rate across all
compared metrics (see `reports/reconciliation_report.csv`), which is strong
evidence the daily aggregation logic in `src/transform.py` is correct.

## 10. SQL Analytics

`sql/03_analytics.sql` contains 10 queries: average PM2.5/PM10 by city, top
10 most polluted cities, monthly and annual PM2.5 trends, year-over-year
change (via `LAG()` window function), AQI category distribution, best/worst
data completeness by city, most polluted individual days, and a cross-city
pollutant comparison. Uses `GROUP BY`, `JOIN`, `CTE`s, `CASE`, `DATE_TRUNC`,
and window functions.

## 11. Setup Instructions

```bash
git clone https://github.com/harry3201/Airquality-etl
cd air_quality_etl
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# edit .env with your local PostgreSQL credentials

# place city_hour.csv and city_day.csv under data/raw/ if not already present
# city_hour.csv is used by the ETL; city_day.csv is used only for reconciliation
```

Then create the schema and tables:

```bash
psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB -f sql/01_create_schema.sql
psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB -f sql/02_create_tables.sql
```

## 12. Execution Instructions

```bash
# Run tests
pytest tests/ -v

# Run the profiling notebook (exploratory only)
jupyter notebook notebooks/01_data_profiling.ipynb

# Run the ETL pipeline
python -m src.pipeline

# Run analytics once data is loaded
psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB -f sql/03_analytics.sql
```

## 13. Project Structure

```
air_quality_etl/
├── data/
│   ├── raw/                    # city_hour.csv, city_day.csv (gitignored)
│   └── processed/              # daily_aggregate.csv (pipeline output)
├── notebooks/
│   └── 01_data_profiling.ipynb # exploratory profiling only
├── src/
│   ├── config.py                # paths, env vars, thresholds
│   ├── extract.py                # raw CSV -> DataFrame
│   ├── validate.py               # 11 reusable data-quality checks
│   ├── transform.py              # hourly -> daily aggregation
│   ├── load.py                   # idempotent PostgreSQL upserts
│   ├── reconcile.py              # compare derived daily vs city_day.csv
│   └── pipeline.py               # orchestrates the full run
├── sql/
│   ├── 01_create_schema.sql
│   ├── 02_create_tables.sql
│   └── 03_analytics.sql
├── tests/
│   └── test_validation.py       # 22 tests on synthetic DataFrames
├── reports/
│   ├── data_quality_report.csv
│   └── reconciliation_report.csv
├── docs/
│   └── data_quality.md
├── requirements.txt
├── .env.example
└── .gitignore
```



## 14.Findings

- The dataset is structurally very clean (no nulls in key columns, no
  duplicate keys, no negative values, no record-level timestamp gaps) — the
  real data-quality challenge is measurement-level missingness and extreme
  values, not structural corruption.
- Xylene (64% missing) and PM10 (42% missing) are the least reliable
  pollutants for city-wide comparison; CO (12% missing) is the most complete.
- 11,135 hourly AQI readings exceed the official 500-point ceiling — flagged
  for investigation rather than silently dropped or capped.
- The independently-derived daily aggregate matches the official
  `city_day.csv` at a ~99.8% rate within a 5% tolerance, validating the
  transformation logic.

## 15. Limitations


- The 5% reconciliation tolerance is a pragmatic choice given the two
  sources' exact aggregation methodology is undocumented; it is not proof of
  bit-for-bit equivalence.
- `source_aqi_bucket` on the daily fact is derived from an *averaged* hourly
  AQI value, not the official sub-index-based daily AQI methodology — it is
  intentionally a convenience label, reconciled against (not assumed equal
  to) the official bucket.
- Suspicious-value thresholds (`src/config.py`) are heuristic, informed by
  the dataset's own distribution and the official AQI scale 

## 16. Future Improvements

- Automated outlier investigation (e.g. flag values that are suspicious *and*
  isolated, vs. part of a multi-city pollution event, which is more likely a
  real spike than a sensor fault).
- A lightweight orchestration layer (e.g. a scheduler) for periodic reruns,
  once new data becomes available — deliberately out of scope for this
  portfolio project per its "no Airflow/Docker/cloud" constraint.
- A small Power BI / matplotlib dashboard on top of `fact_air_quality_daily`.
- Config-driven suspicious-value thresholds per pollutant per city, since
  "normal" pollution levels vary significantly by region.
