"""Dagster definitions entry point for the data orchestrator project."""

from dagster import Definitions
from dagster_dbt import DbtCliResource

from dagster_project.assets.extract import raw_weather_data
from dagster_project.assets.load import staged_weather_data
from dagster_project.assets.transform import dbt_project, dbt_weather_models
from dagster_project.resources.duckdb import DuckDBResource
from dagster_project.resources.weather_api import WeatherAPIClient
from dagster_project.schedules.daily import daily_weather_job, daily_weather_schedule

defs = Definitions(
    assets=[
        raw_weather_data,
        staged_weather_data,
        dbt_weather_models,
    ],
    resources={
        "weather_api": WeatherAPIClient(),
        "duckdb": DuckDBResource(),
        "dbt": DbtCliResource(
            project_dir=dbt_project,
            profiles_dir=dbt_project.project_dir,
        ),
    },
    jobs=[daily_weather_job],
    schedules=[daily_weather_schedule],
)
