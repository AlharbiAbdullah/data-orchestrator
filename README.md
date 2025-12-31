# Data Orchestrator

End-to-end data pipeline demonstrating modern DataOps practices with Dagster orchestration, dbt transformations, and automated data quality checks.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DAGSTER ORCHESTRATOR                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   SCHEDULE (Daily @ 6 AM UTC)                                   │
│        │                                                         │
│        ▼                                                         │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐       │
│   │   EXTRACT   │────▶│    LOAD     │────▶│  TRANSFORM  │       │
│   │ Weather API │     │   DuckDB    │     │    dbt      │       │
│   └─────────────┘     └─────────────┘     └─────────────┘       │
│                                                  │               │
│                                                  ▼               │
│                                           ┌─────────────┐       │
│                                           │  19 TESTS   │       │
│                                           │ (dbt tests) │       │
│                                           └─────────────┘       │
│                                                                  │
│   DAGSTER UI (http://localhost:3333)                            │
│   - Asset lineage graph                                          │
│   - Run history & logs                                           │
│   - Schedule management                                          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Orchestrator | Dagster | Asset-based pipeline management |
| Transformations | dbt | SQL-based data modeling |
| Data Quality | dbt tests | Automated validation |
| Database | DuckDB | Lightweight analytics database |
| API Client | httpx | Weather data extraction |
| Package Manager | UV | Fast Python dependency management |
| Container | Docker | Reproducible deployment |

## Data Flow

### Extract
- **Source**: Open-Meteo Weather API (free, no API key)
- **Cities**: Riyadh, Dubai, London, New York (configurable)
- **Data**: 7 days historical + 7 days forecast
- **Output**: JSON files in `data/raw/`

### Load
- Raw JSON → DuckDB staging tables
- Tables: `raw_hourly_weather`, `raw_daily_weather`

### Transform (dbt)

```
staging/                    intermediate/               marts/
├── stg_hourly_weather  →  ├── int_hourly_enriched  →  ├── fct_daily_weather
└── stg_daily_weather   →  └── int_daily_enriched   →  └── fct_city_comparison
```

## Quick Start

### Option 1: Docker (Recommended)

```bash
# Clone repository
git clone https://github.com/AlharbiAbdullah/data-orchestrator
cd data-orchestrator

# Start services
docker-compose up -d

# Open Dagster UI
open http://localhost:3333
```

### Option 2: Local Development

```bash
# Install dependencies
uv sync

# Start Dagster dev server
uv run dagster dev

# Open Dagster UI
open http://localhost:3000
```

## Project Structure

```
data-orchestrator/
├── pyproject.toml              # Python dependencies (UV)
├── Dockerfile                  # Multi-stage Docker build
├── docker-compose.yml          # Dagster services
│
├── config/
│   └── cities.yml              # Configurable city coordinates
│
├── data/
│   ├── raw/                    # Raw JSON from API
│   └── warehouse/              # DuckDB database
│
├── dagster_project/
│   ├── definitions.py          # Dagster entry point
│   ├── assets/
│   │   ├── extract.py          # Weather API extraction
│   │   ├── load.py             # DuckDB loading
│   │   └── transform.py        # dbt integration
│   ├── resources/
│   │   ├── weather_api.py      # API client resource
│   │   └── duckdb.py           # Database resource
│   └── schedules/
│       └── daily.py            # Daily schedule
│
└── dbt_project/
    ├── dbt_project.yml
    ├── profiles.yml
    └── models/
        ├── staging/            # Clean raw data
        ├── intermediate/       # Enriched with categories
        └── marts/              # Business-ready tables
```

## Configuration

### Adding Cities

Edit `config/cities.yml`:

```yaml
cities:
  tokyo:
    lat: 35.6762
    lon: 139.6503
    timezone: Asia/Tokyo
```

### Schedule

The pipeline runs daily at 6 AM UTC. To change:

```python
# dagster_project/schedules/daily.py
cron_schedule="0 6 * * *"  # Modify cron expression
```

## dbt Models

### Staging Layer
- `stg_hourly_weather`: Cleaned hourly observations
- `stg_daily_weather`: Cleaned daily aggregates

### Intermediate Layer
- `int_hourly_enriched`: + temperature/wind/precipitation categories
- `int_daily_enriched`: + weather conditions, seasons, weekend flags

### Marts Layer
- `fct_daily_weather`: Complete daily metrics per city
- `fct_city_comparison`: City-level statistics and rankings

## Data Quality

19 dbt tests run automatically:
- `not_null` on key columns
- `unique` on city comparison
- `accepted_values` for category enums

## Commands

```bash
# Docker
docker-compose up -d          # Start services
docker-compose down           # Stop services
docker-compose logs -f        # View logs

# Local development
uv sync                       # Install dependencies
uv run dagster dev            # Start Dagster UI
uv run dbt run                # Run dbt models
uv run dbt test               # Run dbt tests

# Manual pipeline run
uv run python -c "
from dagster import materialize
from dagster_project.definitions import defs
# Trigger materialization via UI or API
"
```

## Sample Output

### City Comparison

| City | Avg Temp | Precipitation | Clear Days | Rank |
|------|----------|---------------|------------|------|
| Dubai | 21.3°C | 0.1mm | 13/14 | #1 |
| Riyadh | 15.1°C | 0.0mm | 14/14 | #2 |
| London | 3.1°C | 2.4mm | 13/14 | #3 |
| New York | -0.8°C | 19.6mm | 9/14 | #4 |

## Dagster UI Features

1. **Asset Graph** - Visualize data lineage
2. **Runs** - View execution history
3. **Schedules** - Manage automation
4. **Assets** - Monitor materialization status
5. **Logs** - Debug pipeline issues

## Author

**Abdullah Al Harbi** - Data & AI Engineer

## License

MIT
