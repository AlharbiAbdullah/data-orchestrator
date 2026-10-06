# dbt models

The dbt project is `dbt_project/` (project name `weather_pipeline`). Dagster runs it with
`dbt build`, so every model's tests run right after the model is built.

## Layers

| Layer | Materialized | Schema in DuckDB | Models |
|-------|--------------|------------------|--------|
| Sources | tables, written by the load step | `main` | `raw_hourly_weather`, `raw_daily_weather` |
| Staging | view | `main_staging` | `stg_hourly_weather`, `stg_daily_weather` |
| Intermediate | view | `main_intermediate` | `int_hourly_enriched`, `int_daily_enriched` |
| Marts | table | `main_marts` | `fct_daily_weather`, `fct_city_comparison` |

### Staging

- `stg_hourly_weather`: renamed hourly observations, plus the observation date and hour.
- `stg_daily_weather`: renamed daily aggregates, plus the daily temperature range.

### Intermediate

- `int_hourly_enriched`: adds temperature, precipitation and wind categories, and time of day.
- `int_daily_enriched`: adds average temperature, weather condition (from precipitation),
  temperature variability, season (Northern Hemisphere assumed) and a weekend flag.

### Marts

- `fct_daily_weather`: one row per city and day, the daily data joined to the hourly averages.
- `fct_city_comparison`: one row per city, with temperature, precipitation, wind and humidity
  statistics, day counts, the date range, and ranks for warmest, wettest, windiest and most humid.

## Tests

19 data tests, declared in each layer's `_schema.yml`:

| Layer | Tests |
|-------|-------|
| Staging | `not_null` on city, observation time and temperature (hourly) or max temperature (daily) (6) |
| Intermediate | `not_null` on city and observation time, `accepted_values` on `temp_category` and `weather_condition` (6) |
| Marts | `not_null` on city, date, max and min temperature, and day count; `unique` on `fct_city_comparison.city` (7) |
