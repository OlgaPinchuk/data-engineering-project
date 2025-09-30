{{ config(materialized='view') }}

with src as (
  select
    raw_json as j, 
    source_url,                      
    fetched_at                      
  from {{ source('raw', 'weather_data') }}
  where source_url like '%history.json%' 
),

flat as (
  -- Flatten: forecast.forecastday[].hour[] → one row per hour
  select
    JSON_VALUE(j, '$.location.name')                   as city,
    JSON_VALUE(j, '$.location.country')                as country,
    CAST(JSON_VALUE(j, '$.location.lat') AS FLOAT64)   as lat,
    CAST(JSON_VALUE(j, '$.location.lon') AS FLOAT64)   as lon,

    DATE(JSON_VALUE(day,  '$.date'))                   as day_date,
    CAST(JSON_VALUE(hour, '$.time_epoch') AS INT64)    as time_epoch,
    TIMESTAMP_SECONDS(CAST(JSON_VALUE(hour, '$.time_epoch') AS INT64)) as observed_at,

    SAFE_CAST(JSON_VALUE(hour, '$.temp_c') AS NUMERIC) as temp,
    SAFE_CAST(JSON_VALUE(hour, '$.humidity') AS INT64) as humidity,
    JSON_VALUE(hour, '$.condition.text')               as condition_text,

    source_url,
    fetched_at
  from src,
  UNNEST(JSON_QUERY_ARRAY(j, '$.forecast.forecastday')) as day,
  UNNEST(JSON_QUERY_ARRAY(day, '$.hour'))               as hour
),

dedup as (
  -- Keep the newest record per (lat, lon, time_epoch)
  select
    *,
    TO_HEX(MD5(CONCAT(
      CAST(lat AS STRING), ',', CAST(lon AS STRING), '|', CAST(time_epoch AS STRING)
    ))) as weather_hour_id,                     
    ROW_NUMBER() OVER (
      PARTITION BY
        CAST(lat AS STRING),                    
        CAST(lon AS STRING),                   
        time_epoch
      ORDER BY fetched_at DESC
    ) as rn
  from flat
)

select * except(rn)
from dedup
where rn = 1