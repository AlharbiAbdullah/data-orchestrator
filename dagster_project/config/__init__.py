"""Configuration module for the data orchestrator project."""

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel


class CityConfig(BaseModel):
    """Configuration for a single city."""

    lat: float
    lon: float
    timezone: str


class Settings(BaseModel):
    """Application settings loaded from configuration files."""

    cities: dict[str, CityConfig]

    @classmethod
    def load(cls, config_path: Path | None = None) -> "Settings":
        """
        Load settings from the cities.yml configuration file.

        Args:
            config_path: Optional path to config directory. Defaults to project config/.

        Returns:
            Settings instance with loaded configuration.
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "config"

        cities_file = config_path / "cities.yml"

        with open(cities_file) as f:
            data: dict[str, Any] = yaml.safe_load(f)

        cities = {
            name: CityConfig(**city_data)
            for name, city_data in data.get("cities", {}).items()
        }

        return cls(cities=cities)


def get_settings() -> Settings:
    """Get application settings singleton."""
    return Settings.load()
