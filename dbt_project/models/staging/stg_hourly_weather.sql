{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'raw_hourly_weather') }}
),

renamed as (
    select
        city,
        timestamp as observation_timestamp,
        date_trunc('day', timestamp) as observation_date,
        extract(hour from timestamp) as observation_hour,
        temperature_c,
        humidity_pct,
        precipitation_mm,
        wind_speed_kmh,
        weather_code,
        extracted_at
    from source
)

select * from renamed
