{{
    config(
        materialized='view'
    )
}}

with hourly as (
    select * from {{ ref('stg_hourly_weather') }}
),

enriched as (
    select
        city,
        observation_timestamp,
        observation_date,
        observation_hour,
        temperature_c,
        humidity_pct,
        precipitation_mm,
        wind_speed_kmh,
        weather_code,

        -- Temperature classification
        case
            when temperature_c < 0 then 'freezing'
            when temperature_c < 10 then 'cold'
            when temperature_c < 20 then 'mild'
            when temperature_c < 30 then 'warm'
            else 'hot'
        end as temp_category,

        -- Precipitation classification
        case
            when precipitation_mm = 0 then 'dry'
            when precipitation_mm < 2.5 then 'light'
            when precipitation_mm < 7.5 then 'moderate'
            else 'heavy'
        end as precip_category,

        -- Wind classification (Beaufort scale simplified)
        case
            when wind_speed_kmh < 12 then 'calm'
            when wind_speed_kmh < 29 then 'light'
            when wind_speed_kmh < 50 then 'moderate'
            when wind_speed_kmh < 75 then 'strong'
            else 'storm'
        end as wind_category,

        -- Time of day
        case
            when observation_hour between 6 and 11 then 'morning'
            when observation_hour between 12 and 17 then 'afternoon'
            when observation_hour between 18 and 21 then 'evening'
            else 'night'
        end as time_of_day,

        extracted_at

    from hourly
)

select * from enriched
