{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'raw_daily_weather') }}
),

renamed as (
    select
        city,
        date as observation_date,
        temp_max_c,
        temp_min_c,
        temp_max_c - temp_min_c as temp_range_c,
        precipitation_sum_mm,
        wind_speed_max_kmh,
        extracted_at
    from source
)

select * from renamed
