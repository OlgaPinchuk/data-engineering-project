-- Flatten hourly weather data into version_2_dbt_data.stg_weather
{{ config(
    materialized="table",
    schema="version_2_dbt_data"
) }}

with source as (
    select raw_json
    from {{ source('raw', 'weather_data') }}
),

exploded as (
    select
        cast(parse_timestamp("%Y-%m-%d %H:%M", json_extract_scalar(hour_item, "$.time")) as date) as date,
        extract(hour from parse_timestamp("%Y-%m-%d %H:%M", json_extract_scalar(hour_item, "$.time"))) as hour,
        cast(json_extract_scalar(hour_item, "$.temp_c") as float64) as temp,
        cast(json_extract_scalar(hour_item, "$.humidity") as int64) as humidity,
        cast(json_extract_scalar(hour_item, "$.pressure_mb") as float64) as pressure
    from source
    cross join unnest(json_extract_array(raw_json, "$.forecast.forecastday[0].hour")) as hour_item
)

select *
from exploded
order by date, hour