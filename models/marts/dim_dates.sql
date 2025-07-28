WITH date_series AS (
    SELECT
        CAST(MIN(order_date) AS DATE) AS start_date,
        CAST(MAX(order_date) AS DATE) AS end_date
    FROM {{ ref('stg_all_sales') }}
),
all_dates AS (
    SELECT
        CAST(date_series.start_date + (ROW_NUMBER() OVER () - 1) * INTERVAL '1 day' AS DATE) AS date_day
    FROM date_series
    CROSS JOIN UNNEST(GENERATE_SERIES(1, DATEDIFF('day', date_series.start_date, date_series.end_date) + 1)) AS t(n)
)
SELECT
    date_day,
    YEAR(date_day) AS year,
    MONTH(date_day) AS month,
    DAY(date_day) AS day_of_month,
    DAYOFWEEK(date_day) AS day_of_week,
    WEEKOFYEAR(date_day) AS week_of_year,
    QUARTER(date_day) AS quarter,
    CONCAT(YEAR(date_day), '-', LPAD(CAST(MONTH(date_day) AS VARCHAR), 2, '0')) AS year_month,
    CASE
        WHEN DAYOFWEEK(date_day) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS is_weekend
FROM all_dates
ORDER BY date_day
