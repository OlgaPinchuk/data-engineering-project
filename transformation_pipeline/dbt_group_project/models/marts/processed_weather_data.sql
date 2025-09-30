{{ config(
    materialized='table',
    schema='version_2_dbt_data'
) }}

WITH base AS (
    SELECT
        dt_iso,
        EXTRACT(HOUR FROM PARSE_TIMESTAMP('%Y-%m-%d %H:%M', dt_iso)) AS hour,
        EXTRACT(MONTH FROM PARSE_TIMESTAMP('%Y-%m-%d %H:%M', dt_iso)) AS month,
        temp,
        humidity,
        CAST(pressure AS INT64) AS pressure
    FROM {{ ref('stg_weather_data') }}
),

-- Compute lag features
lagged AS (
    SELECT
        *,
        LAG(temp, 1) OVER (ORDER BY dt_iso, hour) AS temp_lag_1,
        LAG(temp, 3) OVER (ORDER BY dt_iso, hour) AS temp_lag_3
    FROM base
),

-- Compute target: max temp of next 24 hours
target AS (
    SELECT
        *,
        MAX(temp) OVER (
            ORDER BY dt_iso, hour
            ROWS BETWEEN 1 FOLLOWING AND 24 FOLLOWING
        ) AS target_temp
    FROM lagged
)

SELECT *
FROM target
ORDER BY dt_iso, hour