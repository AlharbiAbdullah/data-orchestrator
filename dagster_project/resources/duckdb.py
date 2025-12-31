"""DuckDB resource for data warehouse operations."""

from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import duckdb
from dagster import ConfigurableResource, get_dagster_logger


class DuckDBResource(ConfigurableResource):
    """Resource for DuckDB database connections."""

    database_path: str = "data/warehouse/weather.duckdb"

    def _get_absolute_path(self) -> Path:
        """Get absolute path to the database file."""
        path = Path(self.database_path)
        if not path.is_absolute():
            # Relative to project root
            path = Path(__file__).parent.parent.parent / path
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    @contextmanager
    def get_connection(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """
        Get a DuckDB connection context manager.

        Yields:
            DuckDB connection that auto-closes on exit.
        """
        db_path = self._get_absolute_path()
        logger = get_dagster_logger()
        logger.debug(f"Connecting to DuckDB at {db_path}")

        conn = duckdb.connect(str(db_path))
        try:
            yield conn
        finally:
            conn.close()

    def execute(self, query: str, parameters: list | None = None) -> None:
        """
        Execute a SQL query without returning results.

        Args:
            query: SQL query to execute
            parameters: Optional query parameters
        """
        with self.get_connection() as conn:
            if parameters:
                conn.execute(query, parameters)
            else:
                conn.execute(query)

    def fetch_all(self, query: str, parameters: list | None = None) -> list[tuple]:
        """
        Execute a query and fetch all results.

        Args:
            query: SQL query to execute
            parameters: Optional query parameters

        Returns:
            List of result tuples
        """
        with self.get_connection() as conn:
            if parameters:
                result = conn.execute(query, parameters)
            else:
                result = conn.execute(query)
            return result.fetchall()

    def init_schema(self) -> None:
        """Initialize the database schema for weather data."""
        logger = get_dagster_logger()
        logger.info("Initializing DuckDB schema")

        with self.get_connection() as conn:
            # Raw hourly weather data table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS raw_hourly_weather (
                    city VARCHAR NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    temperature_c DOUBLE,
                    humidity_pct DOUBLE,
                    precipitation_mm DOUBLE,
                    wind_speed_kmh DOUBLE,
                    weather_code INTEGER,
                    extracted_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (city, timestamp)
                )
            """)

            # Raw daily weather data table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS raw_daily_weather (
                    city VARCHAR NOT NULL,
                    date DATE NOT NULL,
                    temp_max_c DOUBLE,
                    temp_min_c DOUBLE,
                    precipitation_sum_mm DOUBLE,
                    wind_speed_max_kmh DOUBLE,
                    extracted_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (city, date)
                )
            """)

            # Extraction metadata table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS extraction_log (
                    extraction_id VARCHAR PRIMARY KEY,
                    extracted_at TIMESTAMP NOT NULL,
                    cities_count INTEGER,
                    hourly_records INTEGER,
                    daily_records INTEGER,
                    source_file VARCHAR
                )
            """)

        logger.info("DuckDB schema initialized successfully")
