# Docker

`docker-compose.yml` builds one image from the `Dockerfile` and runs it as two services.

| Service | Command | Port |
|---------|---------|------|
| `dagster-webserver` | `dagster-webserver -h 0.0.0.0 -p 3000` | host `3333` to container `3000` |
| `dagster-daemon` | `dagster-daemon run` (fires the schedule) | none |

Both mount `dagster_project/`, `config/`, `data/` and `dbt_project/` from the host, and share a
`dagster-storage` volume for `DAGSTER_HOME`. The image does not copy `dbt_project/`, so the
mount is what gives the containers the dbt project.

## Commands

```sh
docker compose up -d       # start both services, UI on localhost:3333
docker compose logs -f     # follow the logs
docker compose down        # stop them
```

## Before the first start

The dbt assets load from `dbt_project/target/manifest.json`. `prepare_if_dev()` in
`dagster_project/assets/transform.py` builds that manifest only under `dagster dev`, and
`target/` is gitignored. On a fresh clone, run `uv run dagster dev` once on the host so the
manifest exists in the mounted folder, then start Compose.
