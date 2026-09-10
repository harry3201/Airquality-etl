# Data Quality Findings — Air Quality India ETL

This document summarizes the real data-quality profile of `city_hour.csv`
(707,875 rows, 26 cities, 2015–2020), based on running
`src/validate.run_full_validation()` against the full dataset. The complete
machine-readable report is at `reports/data_quality_report.csv`.

## Summary

| Area | Result |
|---|---|
| Required columns | All present |
| Data types | All pollutant/AQI columns numeric |
| Null `City` | 0 rows |
| Invalid/missing `Datetime` | 0 rows |
| Duplicate City+Datetime | 0 rows |
| Negative pollutant values | 0 across all 12 pollutants |
| Timestamp gaps | 0% — every city reports every hour within its own active window |
| Daily temporal completeness | 99.82% of City+Date groups have all 24 expected hourly records |

The dataset is structurally clean: no broken keys, no duplicates, no negative
readings, and essentially no missing *records*. The real data-quality story
here is at the **measurement** level, not the **record** level.

## Missing values (never imputed)

| Column | Missing % |
|---|---|
| Xylene | 64.39% |
| PM10 | 41.92% |
| NH3 | 38.50% |
| Toluene | 31.16% |
| Benzene | 23.12% |
| PM2.5 | 20.50% |
| SO2 | 18.42% |
| AQI / AQI_Bucket | 18.23% |
| NOx | 17.41% |
| NO2 | 16.55% |
| NO | 16.48% |
| CO | 12.22% |

**Interpretation:** this reflects real sensor deployment patterns — not every
station has every pollutant sensor installed or working at all times. Xylene
and PM10 in particular should be treated with caution in any city-level
comparison. Per the project's core design principle, none of these values are
imputed. Instead, the daily aggregate carries `observation_count` and
`completeness_pct` so any downstream average can be weighed against how much
data actually backed it.

## Suspicious / extreme values (flagged, not deleted)

No negative values were found anywhere in the dataset. However, a meaningful
number of readings exceed domain-informed "suspicious" thresholds
(see `SUSPICIOUS_VALUE_THRESHOLDS` in `src/config.py`):

| Pollutant | Threshold | Flagged rows |
|---|---|---|
| AQI | > 500 (off official scale) | 11,135 |
| CO | > 200 | 478 |
| NOx | > 400 | 361 |
| NO | > 400 | 250 |
| Xylene | > 100 | 201 |
| NH3 | > 400 | 208 |
| Benzene | > 450 | 155 |
| Toluene | > 450 | 148 |
| O3 | > 250 | 77 |
| NO2 | > 400 | 72 |
| SO2 | > 200 | 0 |
| PM2.5 / PM10 | > 1000 | 0 |

These are **not deleted**. Some genuinely reflect severe pollution episodes
(e.g. Diwali firework nights, crop-burning season); others may be sensor
faults. Distinguishing the two requires domain investigation beyond the
scope of an automated threshold check, so the pipeline surfaces them for a
human/analyst decision rather than making that call silently.

## Timestamp gaps vs. NULL measurements

These are tracked as two distinct concepts:

- **Timestamp gap** = an hourly *record* is entirely absent for a city (i.e.
  no row exists for that City+Datetime at all). Measured against each city's
  own active date range, since cities joined the monitoring network at
  different times (e.g. Ahmedabad from 2015, Ernakulam only from Jan 2020).
  **Result: 0% gap** — once a station is active, it reports essentially every
  hour.
- **NULL measurement** = the hourly record exists, but a specific
  pollutant's value is NULL. This is the dominant form of missingness in the
  dataset (see the table above) and is what `completeness_pct` in the daily
  fact is built to make visible, distinct from record-level gaps.

## Reconciliation outcome (summary)

The daily aggregate built from `city_hour.csv` was compared against the
independently published `city_day.csv` (see `reports/reconciliation_report.csv`
for the full breakdown). Average match rate across all compared metrics:
**~99.8%** within a 5% relative tolerance, which is strong evidence the
aggregation logic in `src/transform.py` is correct and consistent with how
the official daily figures were derived.
