"""dbt transformation assets for weather data pipeline."""

from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

# Path to dbt project
DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt_project"

# Create dbt project instance
dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
)

# Prepare the dbt manifest (generates if needed)
dbt_project.prepare_if_dev()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
)
def dbt_weather_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """Run dbt models for weather transformations."""
    yield from dbt.cli(["build"], context=context).stream()
