{{ config(
    materialized='table',
    schema='version_2_dbt_data'
) }}

WITH target AS (
    SELECT
        dt_iso,
        MAX(temp) OVER (
            ORDER BY dt_iso
            ROWS BETWEEN 1 FOLLOWING AND 24 FOLLOWING
        ) AS target_temp
    FROM {{ ref('stg_weather_data') }}
)

SELECT
    dt_iso,
    target_temp
FROM target
ORDER BY dt_iso