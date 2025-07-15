-- models/marts/dim_dates.sql

-- Simple date dimension table
-- Can be expanded with more date attributes (day_of_week, month_name, etc.)
{{ config(
    materialized='table',
    unique_key='date_sk'
) }}

WITH date_series AS (
SELECT generate_series(
    '2023-01-01'::date, -- Start date (adjust as needed based on your data)
    CURRENT_DATE::date,  -- End date
    '1 day'::interval
)::date AS date_day
)
SELECT
    TO_CHAR(date_day, 'YYYYMMDD')::INT AS date_sk, -- Surrogate key for date
    date_day AS date_actual,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    TO_CHAR(date_day, 'Mon') AS month_name_short,
    EXTRACT(DAY FROM date_day) AS day_of_month,
    EXTRACT(DOW FROM date_day) AS day_of_week_num, -- 0 = Sunday, 6 = Saturday
    CASE EXTRACT(DOW FROM date_day)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_of_week_name,
    (EXTRACT(ISODOW FROM date_day) IN (6, 7)) AS is_weekend -- 1=Monday, 7=Sunday for ISODOW
FROM
    date_series
ORDER BY
    date_day