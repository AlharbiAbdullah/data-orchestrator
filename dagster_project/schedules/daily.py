"""Daily schedule for the weather data pipeline."""

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
    define_asset_job,
)

# Define the daily weather pipeline job
# This selects all assets in the pipeline
daily_weather_job = define_asset_job(
    name="daily_weather_pipeline",
    selection=AssetSelection.all(),
    description="Daily job to extract, load, and transform weather data",
)

# Define the daily schedule
# Runs at 6 AM UTC every day
daily_weather_schedule = ScheduleDefinition(
    job=daily_weather_job,
    cron_schedule="0 6 * * *",  # Every day at 6:00 AM UTC
    description="Runs the complete weather pipeline daily at 6 AM UTC",
    default_status=DefaultScheduleStatus.STOPPED,  # Start stopped, enable in UI
)
