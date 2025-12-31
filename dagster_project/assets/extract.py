"""Extraction assets for weather data pipeline."""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from dagster_project.config import get_settings
from dagster_project.resources.weather_api import WeatherAPIClient


def get_raw_data_path() -> Path:
    """Get the path to the raw data directory."""
    return Path(__file__).parent.parent.parent / "data" / "raw"


@asset(
    description="Raw weather data from Open-Meteo API for all configured cities",
    group_name="extract",
    compute_kind="api",
)
def raw_weather_data(
    context: AssetExecutionContext,
    weather_api: WeatherAPIClient,
) -> MaterializeResult:
    """
    Extract weather data for all configured cities and store as JSON files.

    Fetches hourly and daily weather data from Open-Meteo API for each city
    defined in config/cities.yml. Stores raw JSON responses in data/raw/.
    """
    settings = get_settings()
    raw_path = get_raw_data_path()
    raw_path.mkdir(parents=True, exist_ok=True)

    extraction_time = datetime.now(timezone.utc)
    timestamp = extraction_time.strftime("%Y%m%d_%H%M%S")

    results: dict[str, Any] = {}
    cities_extracted = 0
    total_hourly_records = 0

    for city_name, city_config in settings.cities.items():
        context.log.info(f"Extracting weather data for {city_name}")

        weather_data = weather_api.fetch_weather(
            latitude=city_config.lat,
            longitude=city_config.lon,
        )

        # Add metadata to the response
        weather_data["_metadata"] = {
            "city": city_name,
            "extracted_at": extraction_time.isoformat(),
            "config": {
                "lat": city_config.lat,
                "lon": city_config.lon,
                "timezone": city_config.timezone,
            },
        }

        results[city_name] = weather_data
        cities_extracted += 1

        # Count hourly records
        hourly_data = weather_data.get("hourly", {})
        if "time" in hourly_data:
            total_hourly_records += len(hourly_data["time"])

        context.log.info(f"Successfully extracted data for {city_name}")

    # Save combined results to a single JSON file
    output_file = raw_path / f"weather_{timestamp}.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)

    context.log.info(f"Saved raw weather data to {output_file}")

    return MaterializeResult(
        metadata={
            "cities_extracted": cities_extracted,
            "total_hourly_records": total_hourly_records,
            "output_file": MetadataValue.path(str(output_file)),
            "extraction_time": MetadataValue.text(extraction_time.isoformat()),
            "file_size_bytes": output_file.stat().st_size,
        }
    )
