# GTFS Transit Pipeline

A hybrid batch + streaming data pipeline on MBTA's public GTFS-Realtime feed. The focus is orchestration and CI/CD: a date-parameterized, idempotent Airflow DAG, a file-based Spark Structured Streaming job, and a GitHub Actions pipeline that gates deploys on tests.

The `landing/` folder is the queue.

## Architecture

```
MBTA GTFS-RT feeds (protobuf)
  |  poller/poll_mbta.py  (every 30s)
  v
landing/<YYYY-MM-DD>/trip_updates_<ts>.json   (also the bronze layer)
landing/<YYYY-MM-DD>/_SUCCESS                 (written when the day rolls over)
  |
  |--> streaming/stream_windowed.py   Spark Structured Streaming (file source)
  |      5-min windowed avg delay per route -> hot/route_delay/   (live view)
  |
  '--> Airflow DAG daily_transit_batch (one date partition per run)
         wait_for_day_complete   FileSensor on _SUCCESS
         validate_landing_data   batch/validate_landing.py
         bronze_to_silver        batch/bronze_to_silver.py  -> data/silver/
         silver_to_gold          batch/silver_to_gold.py    -> data/gold/
         load_to_bigquery        batch/load_to_bigquery.py  (GCS staging -> BigQuery)
         post_load_quality_check batch/quality_check.py
```

## Design decisions

- **Idempotent per date.** Every batch job takes `--date` and overwrites that date's output. The BigQuery load uses `WRITE_TRUNCATE` on a partition decorator (`table$YYYYMMDD`), so reruns and backfills replace a day instead of appending.
- **No hardcoded "today".** The DAG passes `{{ ds }}` to every task. The DAG runs in `America/New_York`, matching the poller's date folders.
- **Atomic landing writes.** The poller writes a dot-prefixed temp file and renames it, so Spark never reads a partial file.
- **Live table as upserts.** The streaming job uses update mode with `foreachBatch`, overwriting only the touched `(window_start, route_id)` partitions, so the current window is visible immediately rather than after the watermark closes.
- **Testable transforms.** Transform logic lives in pure functions (DataFrame in, DataFrame out; or plain Python), separate from I/O, and every one has a pytest test.

## Run locally

Requires Python 3.11+ and Java 11+ (for Spark).

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
python -m pytest tests -q
```

Collect data (the MBTA feeds are public, no key needed):

```bash
python -m poller.poll_mbta --interval 30          # writes landing/<date>/
python -m streaming.stream_windowed               # hot/route_delay/, checkpoints/
```

Run the batch steps by hand for a date:

```bash
touch landing/2026-01-15/_SUCCESS                 # normally written by the poller at day rollover
python -m batch.validate_landing --date 2026-01-15
python -m batch.bronze_to_silver --date 2026-01-15
python -m batch.silver_to_gold   --date 2026-01-15
```

Load to BigQuery (needs Google credentials, an existing dataset and an existing bucket):

```bash
export GCP_PROJECT=... BQ_DATASET=... GCS_BUCKET=...
python -m batch.load_to_bigquery --date 2026-01-15
python -m batch.quality_check --date 2026-01-15 --table $GCP_PROJECT.$BQ_DATASET.route_daily_metrics
```

## Run Airflow (Docker)

```bash
cp .env.example .env            # fill in GCP_PROJECT, BQ_DATASET, GCS_BUCKET
cp /path/to/service-account.json secrets/gcp-key.json
docker compose up airflow-init
docker compose up -d
```

Open http://localhost:8080 (admin / admin) and unpause `daily_transit_batch`. The repo is mounted into the containers, so the poller running on your host and the DAG share `landing/` and `data/`. The image is Airflow 2.9.3 on Python 3.11 with Java 17 and PySpark.

## CI/CD

`.github/workflows/ci.yml` runs on every PR and on pushes to `main`:

1. **unit-tests**: `pytest` on the poller, streaming, batch, validation, quality and load code.
2. **dagbag-test**: installs Airflow with its constraints file and checks the DAG imports cleanly, has the expected task order, is date-parameterized, and has failure and SLA callbacks.
3. **deploy** (`main` only, needs both above): packages the deployable code as a build artifact. The final sync step is a placeholder until there is a remote Airflow instance to deploy to.

## Layout

```
poller/       protobuf -> JSON landing files
streaming/    Structured Streaming job
batch/        validate, bronze_to_silver, silver_to_gold, load_to_bigquery, quality_check
dags/         daily_transit_batch
tests/        pytest suite (including DagBag test)
docker/       Airflow image
```
