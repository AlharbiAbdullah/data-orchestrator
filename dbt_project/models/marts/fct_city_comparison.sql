{{
    config(
        materialized='table'
    )
}}

with daily_weather as (
    select * from {{ ref('fct_daily_weather') }}
),

city_stats as (
    select
        city,

        -- Temperature statistics
        avg(temp_avg_c) as avg_temperature_c,
        min(temp_min_c) as min_temperature_c,
        max(temp_max_c) as max_temperature_c,
        avg(temp_range_c) as avg_daily_temp_range_c,

        -- Precipitation statistics
        sum(precipitation_sum_mm) as total_precipitation_mm,
        avg(precipitation_sum_mm) as avg_daily_precipitation_mm,
        count(case when precipitation_sum_mm > 0 then 1 end) as rainy_days,

        -- Wind statistics
        avg(avg_wind_speed_kmh) as avg_wind_speed_kmh,
        max(wind_speed_max_kmh) as max_wind_speed_kmh,

        -- Humidity statistics
        avg(avg_humidity_pct) as avg_humidity_pct,

        -- Day counts
        count(*) as total_days,
        count(case when weather_condition = 'clear' then 1 end) as clear_days,
        count(case when is_weekend then 1 end) as weekend_days,

        -- Date range
        min(observation_date) as first_observation,
        max(observation_date) as last_observation

    from daily_weather
    group by city
),

ranked as (
    select
        *,
        rank() over (order by avg_temperature_c desc) as temp_rank_warmest,
        rank() over (order by total_precipitation_mm desc) as precip_rank_wettest,
        rank() over (order by avg_wind_speed_kmh desc) as wind_rank_windiest,
        rank() over (order by avg_humidity_pct desc) as humidity_rank_most_humid

    from city_stats
)

select * from ranked
