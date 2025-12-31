{{
    config(
        materialized='view'
    )
}}

with daily as (
    select * from {{ ref('stg_daily_weather') }}
),

enriched as (
    select
        city,
        observation_date,
        temp_max_c,
        temp_min_c,
        temp_range_c,
        (temp_max_c + temp_min_c) / 2 as temp_avg_c,
        precipitation_sum_mm,
        wind_speed_max_kmh,

        -- Weather condition based on precipitation
        case
            when precipitation_sum_mm = 0 then 'clear'
            when precipitation_sum_mm < 5 then 'light_rain'
            when precipitation_sum_mm < 20 then 'rainy'
            else 'heavy_rain'
        end as weather_condition,

        -- Day classification
        case
            when temp_range_c > 15 then 'high_variability'
            when temp_range_c > 10 then 'moderate_variability'
            else 'stable'
        end as temp_variability,

        -- Season (Northern Hemisphere assumed)
        case
            when extract(month from observation_date) in (12, 1, 2) then 'winter'
            when extract(month from observation_date) in (3, 4, 5) then 'spring'
            when extract(month from observation_date) in (6, 7, 8) then 'summer'
            else 'autumn'
        end as season,

        extract(dow from observation_date) as day_of_week,
        case
            when extract(dow from observation_date) in (0, 6) then true
            else false
        end as is_weekend,

        extracted_at

    from daily
)

select * from enriched
