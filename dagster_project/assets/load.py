"""Load assets for inserting raw weather data into DuckDB."""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext, AssetIn, MaterializeResult, MetadataValue, asset

from dagster_project.resources.duckdb import DuckDBResource


def get_raw_data_path() -> Path:
    """Get the path to the raw data directory."""
    return Path(__file__).parent.parent.parent / "data" / "raw"


def get_latest_raw_file() -> Path | None:
    """Get the most recent raw weather JSON file."""
    raw_path = get_raw_data_path()
    json_files = list(raw_path.glob("weather_*.json"))

    if not json_files:
        return None

    return max(json_files, key=lambda f: f.stat().st_mtime)


@asset(
    deps=["raw_weather_data"],
    description="Load raw weather data into DuckDB staging tables",
    group_name="load",
    compute_kind="duckdb",
)
def staged_weather_data(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
) -> MaterializeResult:
    """
    Load the latest raw weather JSON into DuckDB tables.

    Reads the most recent extraction file and upserts data into:
    - raw_hourly_weather: Hourly observations
    - raw_daily_weather: Daily aggregates
    - extraction_log: Metadata about the load
    """
    # Initialize schema if needed
    duckdb.init_schema()

    # Find latest raw file
    raw_file = get_latest_raw_file()
    if raw_file is None:
        raise ValueError("No raw weather files found in data/raw/")

    context.log.info(f"Loading data from {raw_file}")

    with open(raw_file) as f:
        raw_data: dict[str, Any] = json.load(f)

    extraction_id = str(uuid.uuid4())
    load_time = datetime.now(timezone.utc)

    total_hourly = 0
    total_daily = 0

    with duckdb.get_connection() as conn:
        for city_name, city_data in raw_data.items():
            metadata = city_data.get("_metadata", {})
            extracted_at = metadata.get("extracted_at", load_time.isoformat())

            # Load hourly data
            hourly = city_data.get("hourly", {})
            times = hourly.get("time", [])
            temps = hourly.get("temperature_2m", [])
            humidity = hourly.get("relative_humidity_2m", [])
            precip = hourly.get("precipitation", [])
            wind = hourly.get("wind_speed_10m", [])
            weather_codes = hourly.get("weather_code", [])

            for i, time_str in enumerate(times):
                conn.execute("""
                    INSERT OR REPLACE INTO raw_hourly_weather
                    (city, timestamp, temperature_c, humidity_pct,
                     precipitation_mm, wind_speed_kmh, weather_code, extracted_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    city_name,
                    time_str,
                    temps[i] if i < len(temps) else None,
                    humidity[i] if i < len(humidity) else None,
                    precip[i] if i < len(precip) else None,
                    wind[i] if i < len(wind) else None,
                    weather_codes[i] if i < len(weather_codes) else None,
                    extracted_at,
                ])
                total_hourly += 1

            # Load daily data
            daily = city_data.get("daily", {})
            dates = daily.get("time", [])
            temp_max = daily.get("temperature_2m_max", [])
            temp_min = daily.get("temperature_2m_min", [])
            precip_sum = daily.get("precipitation_sum", [])
            wind_max = daily.get("wind_speed_10m_max", [])

            for i, date_str in enumerate(dates):
                conn.execute("""
                    INSERT OR REPLACE INTO raw_daily_weather
                    (city, date, temp_max_c, temp_min_c,
                     precipitation_sum_mm, wind_speed_max_kmh, extracted_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, [
                    city_name,
                    date_str,
                    temp_max[i] if i < len(temp_max) else None,
                    temp_min[i] if i < len(temp_min) else None,
                    precip_sum[i] if i < len(precip_sum) else None,
                    wind_max[i] if i < len(wind_max) else None,
                    extracted_at,
                ])
                total_daily += 1

            context.log.info(f"Loaded data for {city_name}")

        # Log extraction metadata
        conn.execute("""
            INSERT INTO extraction_log
            (extraction_id, extracted_at, cities_count, hourly_records,
             daily_records, source_file)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            extraction_id,
            load_time.isoformat(),
            len(raw_data),
            total_hourly,
            total_daily,
            str(raw_file.name),
        ])

    context.log.info(
        f"Loaded {total_hourly} hourly and {total_daily} daily records"
    )

    return MaterializeResult(
        metadata={
            "extraction_id": extraction_id,
            "cities_loaded": len(raw_data),
            "hourly_records": total_hourly,
            "daily_records": total_daily,
            "source_file": MetadataValue.path(str(raw_file)),
            "load_time": MetadataValue.text(load_time.isoformat()),
        }
    )
