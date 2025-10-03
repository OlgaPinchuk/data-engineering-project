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
        * ,
        LAG(temp, 1) OVER (ORDER BY dt_iso, hour) AS temp_lag_1,
        LAG(temp, 3) OVER (ORDER BY dt_iso, hour) AS temp_lag_3
    FROM base
)

SELECT
    dt_iso,
    hour,
    month,
    temp,
    humidity,
    pressure,
    temp_lag_1,
    temp_lag_3
FROM lagged
ORDER BY dt_iso, hour