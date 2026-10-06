# Configuration

## Cities

The pipeline fetches every city in `config/cities.yml`: Riyadh, Dubai, London and New York
today. To add one, add an entry with its coordinates and timezone:

```yaml
cities:
  tokyo:
    lat: 35.6762
    lon: 139.6503
    timezone: Asia/Tokyo
```

The file is validated with Pydantic on load (`dagster_project/config/__init__.py`).

## Schedule

`daily_weather_pipeline_schedule` runs the `daily_weather_pipeline` job, which selects every
asset, at 06:00 UTC. It ships stopped (`DefaultScheduleStatus.STOPPED`): turn it on in the
Dagster UI under Schedules. A daemon must be running for it to fire: `dagster dev` starts one,
and so does the `dagster-daemon` Compose service.

To change the time, edit the cron expression in `dagster_project/schedules/daily.py`:

```python
cron_schedule="0 6 * * *",  # every day at 06:00 UTC
```

## API window

`WeatherAPIClient.fetch_weather` (`dagster_project/resources/weather_api.py`) asks for
`past_days=7` and `forecast_days=7`, with `timezone=auto`, so the times are local to each city.

| Grain | Fields |
|-------|--------|
| Hourly | `temperature_2m`, `relative_humidity_2m`, `precipitation`, `wind_speed_10m`, `weather_code` |
| Daily | `temperature_2m_max`, `temperature_2m_min`, `precipitation_sum`, `wind_speed_10m_max` |

The request timeout is 30 seconds.

## Files

| What | Path |
|------|------|
| Raw extracts | `data/raw/weather_<YYYYMMDD_HHMMSS>.json`, one per run, all cities in one file |
| Warehouse | `data/warehouse/weather.duckdb` (`DuckDBResource.database_path`) |
| dbt profile | `dbt_project/profiles.yml`, target `dev`, path `../data/warehouse/weather.duckdb` |

The load step reads only the newest raw file. It upserts into `raw_hourly_weather` (key: city,
timestamp) and `raw_daily_weather` (key: city, date), and adds one row per load to
`extraction_log`.
