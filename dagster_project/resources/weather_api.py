"""Weather API client resource for Open-Meteo API."""

from typing import Any

import httpx
from dagster import ConfigurableResource, get_dagster_logger


class WeatherAPIClient(ConfigurableResource):
    """Resource for fetching weather data from Open-Meteo API."""

    base_url: str = "https://api.open-meteo.com/v1/forecast"
    timeout: int = 30

    def fetch_weather(
        self,
        latitude: float,
        longitude: float,
        past_days: int = 7,
        forecast_days: int = 7,
    ) -> dict[str, Any]:
        """
        Fetch weather data for a specific location.

        Args:
            latitude: Location latitude
            longitude: Location longitude
            past_days: Number of past days to fetch
            forecast_days: Number of forecast days to fetch

        Returns:
            Weather data dictionary from API response
        """
        logger = get_dagster_logger()

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": ",".join([
                "temperature_2m",
                "relative_humidity_2m",
                "precipitation",
                "wind_speed_10m",
                "weather_code",
            ]),
            "daily": ",".join([
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_sum",
                "wind_speed_10m_max",
            ]),
            "timezone": "auto",
            "past_days": past_days,
            "forecast_days": forecast_days,
        }

        logger.info(f"Fetching weather for lat={latitude}, lon={longitude}")

        with httpx.Client(timeout=self.timeout) as client:
            response = client.get(self.base_url, params=params)
            response.raise_for_status()

        return response.json()
