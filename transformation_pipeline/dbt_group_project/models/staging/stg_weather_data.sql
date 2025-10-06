{{ config(
    materialized='table',
    schema='version_2_dbt_data'
) }}

WITH source AS (
  SELECT raw_json
  FROM {{ source('raw', 'weather_data') }}
),

-- Extract location info
metadata AS (
  SELECT
    json_value(raw_json, '$.location.name') AS city_name,
    SAFE_CAST(json_value(raw_json, '$.location.lat') AS FLOAT64) AS lat,
    SAFE_CAST(json_value(raw_json, '$.location.lon') AS FLOAT64) AS lon,
    raw_json
  FROM source
),

-- Extract forecastday array
forecast AS (
  SELECT
    json_query_array(raw_json, '$.forecast.forecastday') AS forecast_days,
    city_name,
    lat,
    lon
  FROM metadata
),

-- Unnest each forecastday and keep daily min/max temps
forecast_unnested AS (
  SELECT
    day AS day_record,
    SAFE_CAST(json_value(day, '$.day.maxtemp_c') AS FLOAT64) AS temp_max,
    SAFE_CAST(json_value(day, '$.day.mintemp_c') AS FLOAT64) AS temp_min,
    city_name,
    lat,
    lon
  FROM forecast, UNNEST(forecast_days) AS day
),

-- Unnest hours inside each forecastday
flattened AS (
  SELECT
    hour AS hour_record,
    temp_max,
    temp_min,
    city_name,
    lat,
    lon
  FROM forecast_unnested, UNNEST(json_query_array(day_record, '$.hour')) AS hour
)

SELECT
  SAFE_CAST(json_value(hour_record, '$.time_epoch') AS INT64) AS dt,
  json_value(hour_record, '$.time') AS dt_iso,
  city_name,
  lat,
  lon,

  SAFE_CAST(json_value(hour_record, '$.temp_c') AS FLOAT64) AS temp,
  SAFE_CAST(json_value(hour_record, '$.feelslike_c') AS FLOAT64) AS feels_like,
  temp_min,
  temp_max,

  SAFE_CAST(json_value(hour_record, '$.pressure_mb') AS FLOAT64) AS pressure,
  SAFE_CAST(json_value(hour_record, '$.humidity') AS INT64) AS humidity,
  SAFE_CAST(json_value(hour_record, '$.vis_km') AS FLOAT64) AS visibility,
  SAFE_CAST(json_value(hour_record, '$.dewpoint_c') AS FLOAT64) AS dew_point,

  SAFE_CAST(json_value(hour_record, '$.wind_kph') AS FLOAT64) AS wind_speed,
  SAFE_CAST(json_value(hour_record, '$.wind_degree') AS INT64) AS wind_deg,
  SAFE_CAST(json_value(hour_record, '$.cloud') AS INT64) AS clouds_all,
  SAFE_CAST(json_value(hour_record, '$.condition.code') AS INT64) AS weather_id,
  json_value(hour_record, '$.condition.text') AS weather_description

FROM flattened
ORDER BY dt