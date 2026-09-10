-- Air Quality India ETL — Analytical queries
-- Run after the pipeline has loaded fact_air_quality_hourly and
-- fact_air_quality_daily. Demonstrates GROUP BY, JOIN, CTEs, CASE,
-- DATE_TRUNC, and window functions.

SET search_path TO air_quality, public;

-- -----------------------------------------------------------------------
-- 1. Average PM2.5 by city
-- -----------------------------------------------------------------------
SELECT
    c.city_name,
    ROUND(AVG(d.avg_pm25), 2) AS avg_pm25,
    COUNT(*)                  AS days_observed
FROM fact_air_quality_daily d
JOIN dim_city c ON c.city_id = d.city_id
WHERE d.avg_pm25 IS NOT NULL
GROUP BY c.city_name
ORDER BY avg_pm25 DESC;


-- -----------------------------------------------------------------------
-- 2. Average PM10 by city
-- -----------------------------------------------------------------------
SELECT
    c.city_name,
    ROUND(AVG(d.avg_pm10), 2) AS avg_pm10,
    COUNT(*)                  AS days_observed
FROM fact_air_quality_daily d
JOIN dim_city c ON c.city_id = d.city_id
WHERE d.avg_pm10 IS NOT NULL
GROUP BY c.city_name
ORDER BY avg_pm10 DESC;


-- -----------------------------------------------------------------------
-- 3. Top 10 most polluted cities (composite: avg PM2.5 as the primary
--    driver of India's AQI in most cities)
-- -----------------------------------------------------------------------
SELECT
    c.city_name,
    ROUND(AVG(d.avg_pm25), 2)     AS avg_pm25,
    ROUND(AVG(d.source_aqi), 2)   AS avg_aqi
FROM fact_air_quality_daily d
JOIN dim_city c ON c.city_id = d.city_id
WHERE d.avg_pm25 IS NOT NULL
GROUP BY c.city_name
ORDER BY avg_pm25 DESC
LIMIT 10;


-- -----------------------------------------------------------------------
-- 4. Monthly PM2.5 trend (all cities combined)
-- -----------------------------------------------------------------------
SELECT
    DATE_TRUNC('month', d.date)::DATE AS month,
    ROUND(AVG(d.avg_pm25), 2)         AS avg_pm25
FROM fact_air_quality_daily d
WHERE d.avg_pm25 IS NOT NULL
GROUP BY DATE_TRUNC('month', d.date)
ORDER BY month;


-- -----------------------------------------------------------------------
-- 5. Annual PM2.5 trend (all cities combined)
-- -----------------------------------------------------------------------
SELECT
    DATE_TRUNC('year', d.date)::DATE AS year,
    ROUND(AVG(d.avg_pm25), 2)        AS avg_pm25
FROM fact_air_quality_daily d
WHERE d.avg_pm25 IS NOT NULL
GROUP BY DATE_TRUNC('year', d.date)
ORDER BY year;


-- -----------------------------------------------------------------------
-- 6. Year-over-year pollution change (PM2.5), per city, using a window
--    function to compare each year against the previous year
-- -----------------------------------------------------------------------
WITH yearly AS (
    SELECT
        c.city_name,
        EXTRACT(YEAR FROM d.date)::INT AS year,
        AVG(d.avg_pm25)                AS avg_pm25
    FROM fact_air_quality_daily d
    JOIN dim_city c ON c.city_id = d.city_id
    WHERE d.avg_pm25 IS NOT NULL
    GROUP BY c.city_name, EXTRACT(YEAR FROM d.date)
)
SELECT
    city_name,
    year,
    ROUND(avg_pm25, 2)                                             AS avg_pm25,
    ROUND(avg_pm25 - LAG(avg_pm25) OVER (
        PARTITION BY city_name ORDER BY year
    ), 2)                                                           AS change_from_prior_year,
    ROUND(
        (avg_pm25 - LAG(avg_pm25) OVER (PARTITION BY city_name ORDER BY year))
        / NULLIF(LAG(avg_pm25) OVER (PARTITION BY city_name ORDER BY year), 0) * 100,
    2)                                                               AS pct_change_from_prior_year
FROM yearly
ORDER BY city_name, year;


-- -----------------------------------------------------------------------
-- 7. AQI category distribution (based on source_aqi_bucket)
-- -----------------------------------------------------------------------
SELECT
    COALESCE(source_aqi_bucket, 'Unknown') AS aqi_bucket,
    COUNT(*)                                AS day_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_days
FROM fact_air_quality_daily
GROUP BY source_aqi_bucket
ORDER BY day_count DESC;


-- -----------------------------------------------------------------------
-- 8. Cities with best/worst data completeness
-- -----------------------------------------------------------------------
SELECT
    c.city_name,
    ROUND(AVG(d.completeness_pct), 2) AS avg_completeness_pct,
    CASE
        WHEN AVG(d.completeness_pct) >= 90 THEN 'Excellent'
        WHEN AVG(d.completeness_pct) >= 70 THEN 'Good'
        WHEN AVG(d.completeness_pct) >= 50 THEN 'Fair'
        ELSE 'Poor'
    END AS completeness_grade
FROM fact_air_quality_daily d
JOIN dim_city c ON c.city_id = d.city_id
GROUP BY c.city_name
ORDER BY avg_completeness_pct DESC;


-- -----------------------------------------------------------------------
-- 9. Most polluted periods: top 10 individual City+Date days by PM2.5
-- -----------------------------------------------------------------------
SELECT
    c.city_name,
    d.date,
    d.avg_pm25,
    d.source_aqi,
    d.source_aqi_bucket
FROM fact_air_quality_daily d
JOIN dim_city c ON c.city_id = d.city_id
WHERE d.avg_pm25 IS NOT NULL
ORDER BY d.avg_pm25 DESC
LIMIT 10;


-- -----------------------------------------------------------------------
-- 10. Comparison of pollutant levels across cities (pivoted summary)
-- -----------------------------------------------------------------------
SELECT
    c.city_name,
    ROUND(AVG(d.avg_pm25), 2) AS avg_pm25,
    ROUND(AVG(d.avg_pm10), 2) AS avg_pm10,
    ROUND(AVG(d.avg_no2), 2)  AS avg_no2,
    ROUND(AVG(d.avg_so2), 2)  AS avg_so2,
    ROUND(AVG(d.avg_co), 2)   AS avg_co,
    ROUND(AVG(d.avg_o3), 2)   AS avg_o3
FROM fact_air_quality_daily d
JOIN dim_city c ON c.city_id = d.city_id
GROUP BY c.city_name
ORDER BY c.city_name;
