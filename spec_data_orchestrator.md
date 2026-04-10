# Data Orchestrator Project Specification

## Project Overview

**Name:** `data-orchestrator`
**Focus:** DataOps, Pipeline Orchestration, Data Quality
**Dataset:** Weather API (Open-Meteo)
**GitHub Repo:** `https://github.com/AlharbiAbdullah/data-orchestrator`

### Goal
Build an end-to-end data pipeline with Dagster orchestration, demonstrating modern DataOps practices including scheduling, monitoring, data quality, and observability.

### What You'll Showcase
- Pipeline orchestration with Dagster
- API data ingestion
- dbt integration
- Data quality checks
- Scheduling and monitoring
- Asset-based data engineering

---

## Dataset: Weather API

**Source:** https://open-meteo.com/en/docs

**Why Weather API:**
- Free, no API key required
- Real-time and historical data
- Perfect for demonstrating scheduled pipelines
- Rich data: temperature, precipitation, wind, etc.

**Data Points:**
- Temperature (hourly/daily)
- Precipitation
- Wind speed and direction
- Humidity
- Weather codes

**Locations:** Multiple cities (Riyadh, Dubai, London, NYC)

---

## Tech Stack

| Component | Technology | Why |
|-----------|------------|-----|
| Orchestrator | Dagster | Modern, asset-based, great UI |
| Transformations | dbt | Industry standard |
| Database | DuckDB | Lightweight, fast |
| API Client | httpx | Modern async HTTP |
| Quality | Great Expectations | Comprehensive validation |
| Python | 3.11+ | Modern features |
| Package Mgmt | UV | Fast, modern |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DAGSTER ORCHESTRATOR                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   SCHEDULE (Daily @ 6 AM)                                       │
│        │                                                         │
│        ▼                                                         │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐      │
│   │   EXTRACT   │────►│  TRANSFORM  │────►│    LOAD     │      │
│   │ Weather API │     │    dbt      │     │   DuckDB    │      │
│   └─────────────┘     └─────────────┘     └─────────────┘      │
│        │                    │                    │               │
│        ▼                    ▼                    ▼               │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐      │
│   │   QUALITY   │     │   QUALITY   │     │   QUALITY   │      │
│   │    CHECK    │     │    CHECK    │     │    CHECK    │      │
│   └─────────────┘     └─────────────┘     └─────────────┘      │
│                                                                  │
│   DAGSTER UI ───────────────────────────────────────────────    │
│   - Asset lineage                                                │
│   - Run history                                                  │
│   - Logs & metrics                                               │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
data-orchestrator/
├── README.md                 # Project documentation
├── pyproject.toml           # Python dependencies (UV)
├── .gitignore
│
├── data/
│   ├── raw/                 # Raw API responses (JSON)
│   └── warehouse/           # DuckDB database
│
├── dagster_project/
│   ├── __init__.py
│   ├── definitions.py       # Dagster definitions entry point
│   │
│   ├── assets/
│   │   ├── __init__.py
│   │   ├── extract.py       # API extraction assets
│   │   ├── transform.py     # dbt transformation assets
│   │   └── quality.py       # Data quality assets
│   │
│   ├── resources/
│   │   ├── __init__.py
│   │   ├── weather_api.py   # Weather API client
│   │   └── duckdb.py        # DuckDB connection
│   │
│   ├── jobs/
│   │   ├── __init__.py
│   │   └── daily_weather.py # Daily pipeline job
│   │
│   └── schedules/
│       ├── __init__.py
│       └── daily.py         # Daily schedule definition
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   │
│   └── models/
│       ├── staging/
│       │   └── stg_weather_raw.sql
│       ├── intermediate/
│       │   └── int_weather_enriched.sql
│       └── marts/
│           ├── fct_daily_weather.sql
│           └── fct_city_comparison.sql
│
├── great_expectations/
│   ├── great_expectations.yml
│   └── expectations/
│       └── weather_suite.json
│
├── scripts/
│   ├── setup.py             # Initialize project
│   └── backfill.py          # Historical data backfill
│
└── docs/
    └── architecture.md
```

---

## Implementation Steps

### Phase 1: Setup (Day 1)
1. [ ] Initialize repository with UV
2. [ ] Set up Dagster project structure
3. [ ] Configure DuckDB resource
4. [ ] Create Weather API client resource
5. [ ] Test basic API connection

### Phase 2: Extraction Assets (Day 1)
1. [ ] Create `raw_weather_data` asset
2. [ ] Implement API pagination/batching
3. [ ] Add multiple city support
4. [ ] Store raw JSON responses
5. [ ] Add extraction metadata

### Phase 3: dbt Integration (Day 2)
1. [ ] Set up dbt project with DuckDB
2. [ ] Create dagster-dbt integration
3. [ ] Build staging models
4. [ ] Build intermediate models
5. [ ] Build mart models

### Phase 4: Data Quality (Day 2)
1. [ ] Set up Great Expectations
2. [ ] Create expectation suite for weather data
3. [ ] Integrate quality checks as Dagster assets
4. [ ] Add quality gates (fail pipeline on bad data)
5. [ ] Create quality dashboard/report

### Phase 5: Scheduling & Polish (Day 3)
1. [ ] Create daily schedule
2. [ ] Implement backfill capability
3. [ ] Add alerting on failures
4. [ ] Write comprehensive README
5. [ ] Record demo of Dagster UI

---

## Key Code

### Dagster Asset: Extract Weather
```python
# dagster_project/assets/extract.py
from dagster import asset, AssetExecutionContext
import httpx
from datetime import datetime, timedelta

CITIES = {
    "riyadh": {"lat": 24.7136, "lon": 46.6753},
    "dubai": {"lat": 25.2048, "lon": 55.2708},
    "london": {"lat": 51.5074, "lon": -0.1278},
    "new_york": {"lat": 40.7128, "lon": -74.0060},
}

@asset(
    description="Raw weather data from Open-Meteo API",
    group_name="extract",
)
def raw_weather_data(context: AssetExecutionContext) -> dict:
    """Extract weather data for multiple cities."""
    results = {}

    for city, coords in CITIES.items():
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "hourly": "temperature_2m,precipitation,windspeed_10m,humidity_2m",
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
            "timezone": "auto",
            "past_days": 7,
            "forecast_days": 7,
        }

        response = httpx.get(url, params=params)
        response.raise_for_status()

        results[city] = response.json()
        context.log.info(f"Extracted weather data for {city}")

    return results
```

### Dagster Asset: dbt Models
```python
# dagster_project/assets/transform.py
from dagster import asset, AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from pathlib import Path

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt_project"

@dbt_assets(manifest=DBT_PROJECT_DIR / "target" / "manifest.json")
def dbt_weather_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """Run dbt models for weather transformations."""
    yield from dbt.cli(["run"], context=context).stream()
```

### Dagster Asset: Data Quality
```python
# dagster_project/assets/quality.py
from dagster import asset, AssetExecutionContext, AssetIn

@asset(
    ins={"weather_data": AssetIn("raw_weather_data")},
    description="Data quality checks for weather data",
    group_name="quality",
)
def weather_quality_check(
    context: AssetExecutionContext,
    weather_data: dict
) -> dict:
    """Validate weather data quality."""
    issues = []

    for city, data in weather_data.items():
        # Check for missing temperatures
        temps = data.get("hourly", {}).get("temperature_2m", [])
        null_count = sum(1 for t in temps if t is None)

        if null_count > len(temps) * 0.1:  # More than 10% missing
            issues.append(f"{city}: {null_count} missing temperature readings")

        # Check for unreasonable values
        valid_temps = [t for t in temps if t is not None]
        if valid_temps:
            if min(valid_temps) < -50 or max(valid_temps) > 60:
                issues.append(f"{city}: Temperature out of range")

    if issues:
        context.log.warning(f"Quality issues found: {issues}")
    else:
        context.log.info("All quality checks passed")

    return {
        "passed": len(issues) == 0,
        "issues": issues,
        "checked_at": datetime.now().isoformat(),
    }
```

### Dagster Schedule
```python
# dagster_project/schedules/daily.py
from dagster import ScheduleDefinition, define_asset_job

daily_weather_job = define_asset_job(
    name="daily_weather_pipeline",
    selection="*",  # All assets
)

daily_weather_schedule = ScheduleDefinition(
    job=daily_weather_job,
    cron_schedule="0 6 * * *",  # Every day at 6 AM
    description="Daily weather data pipeline",
)
```

### dbt Model: Daily Weather
```sql
-- dbt_project/models/marts/fct_daily_weather.sql
with weather as (
    select * from {{ ref('int_weather_enriched') }}
)

select
    city,
    date,

    -- Temperature
    avg_temperature_c,
    max_temperature_c,
    min_temperature_c,

    -- Precipitation
    total_precipitation_mm,

    -- Conditions
    avg_humidity_pct,
    avg_wind_speed_kmh,

    -- Derived
    max_temperature_c - min_temperature_c as temperature_range_c,
    case
        when total_precipitation_mm > 10 then 'Heavy Rain'
        when total_precipitation_mm > 1 then 'Light Rain'
        else 'Dry'
    end as precipitation_category

from weather
```

---

## Dagster UI Features to Demo

1. **Asset Lineage Graph** - Show data flow from API → dbt → quality
2. **Run History** - Show successful/failed runs
3. **Asset Materialization** - Show when data was last updated
4. **Logs** - Show detailed execution logs
5. **Schedules** - Show daily schedule configuration

---

## README Template

```markdown
# Data Orchestrator

End-to-end data pipeline with Dagster orchestration, dbt transformations, and data quality checks.

## Architecture

[Architecture diagram here]

## Tech Stack
- **Orchestrator:** Dagster
- **Transformations:** dbt
- **Quality:** Great Expectations
- **Database:** DuckDB

## Quick Start

```bash
# Clone repository
git clone https://github.com/AlharbiAbdullah/data-orchestrator
cd data-orchestrator

# Install dependencies
uv sync

# Start Dagster UI
uv run dagster dev

# Open browser: http://localhost:3000
```

## Pipeline

| Stage | Description |
|-------|-------------|
| Extract | Fetch weather data from Open-Meteo API |
| Transform | Clean and enrich with dbt |
| Quality | Validate with Great Expectations |
| Load | Store in DuckDB |

## Schedule
- **Frequency:** Daily at 6 AM
- **Backfill:** Supports historical backfill

## Author
Abdullah Al Harbi - Data & AI Engineer
```

---

## Success Criteria

- [ ] Dagster project with asset-based pipeline
- [ ] API extraction for multiple cities
- [ ] dbt models (staging → marts)
- [ ] Data quality checks integrated
- [ ] Daily schedule configured
- [ ] Dagster UI accessible with lineage view
- [ ] README with setup instructions
