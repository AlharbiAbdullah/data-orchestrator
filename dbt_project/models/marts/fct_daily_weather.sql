{{
    config(
        materialized='table'
    )
}}

with daily as (
    select * from {{ ref('int_daily_enriched') }}
),

hourly_agg as (
    select
        city,
        observation_date,
        avg(temperature_c) as avg_temp_c,
        avg(humidity_pct) as avg_humidity_pct,
        sum(precipitation_mm) as total_precip_mm,
        avg(wind_speed_kmh) as avg_wind_speed_kmh,
        count(*) as observation_count
    from {{ ref('int_hourly_enriched') }}
    group by city, observation_date
),

final as (
    select
        d.city,
        d.observation_date,

        -- Temperature metrics
        d.temp_max_c,
        d.temp_min_c,
        d.temp_avg_c,
        d.temp_range_c,
        h.avg_temp_c as hourly_avg_temp_c,

        -- Precipitation
        d.precipitation_sum_mm,
        coalesce(h.total_precip_mm, 0) as hourly_total_precip_mm,

        -- Wind
        d.wind_speed_max_kmh,
        h.avg_wind_speed_kmh,

        -- Humidity
        h.avg_humidity_pct,

        -- Classifications
        d.weather_condition,
        d.temp_variability,
        d.season,
        d.day_of_week,
        d.is_weekend,

        -- Metadata
        h.observation_count as hourly_observations,
        d.extracted_at

    from daily d
    left join hourly_agg h
        on d.city = h.city
        and d.observation_date = h.observation_date
)

select * from final
