# GTFS Transit Pipeline — Project Context for Claude Code

## What this project is
A portfolio data engineering project demonstrating a **hybrid batch + streaming**
pipeline built on MBTA's public GTFS-Realtime feed. Primary goal: show CI/CD
and orchestration skills (not just "another medallion pipeline").

This is a sibling project to an existing GCP-based cafe/bakery batch pipeline —
same medallion conventions (bronze/silver/gold), same PySpark-local-mode style,
but this one adds a real streaming layer and a CI/CD story on top.

## Architecture (end to end)
```
MBTA GTFS-RT feed (protobuf, polled every N seconds)
  -> poller script decodes protobuf -> writes JSON files to landing/<date>/
  -> poller writes a landing/<date>/_SUCCESS marker when the day rolls over
  -> Spark Structured Streaming (readStream on landing/ folder, NO Kafka/broker)
       -> windowed aggregation (e.g. avg delay per route per 5-min window)
       -> writes to a "hot" table (live view)
  -> landing/ JSON files double as the bronze layer for nightly batch
  -> Airflow DAG `daily_transit_batch` (daily, date-parameterized via logical_date):
       1. wait_for_day_complete   (FileSensor on _SUCCESS marker)
       2. validate_landing_data   (schema/row-count sanity check)
       3. bronze_to_silver        (PySpark, local mode)
       4. silver_to_gold          (PySpark, local mode)
       5. load_to_bigquery
       6. post_load_quality_check (row counts / null checks)
       + on_failure_callback / SLA alert
  -> GitHub Actions CI/CD:
       - dagbag import test (catches DAG syntax errors / cycles)
       - unit tests on transform functions (pytest)
       - runs before anything deploys to the Airflow instance
```

Explicitly **not** using Kafka or any message broker — the filesystem (landing
folder) is the queue. Keep this decision in mind: don't suggest Kafka-specific
patterns (consumer groups, partitions, topics) anywhere in this codebase.

## Tech stack
- Python, PySpark (local mode, `local[*]`) — same as the cafe/bakery project
- Spark Structured Streaming — file-source streaming, not Kafka-source
- Airflow (Docker-based, Postgres + LocalExecutor — same setup pattern as
  the cafe/bakery project's Airflow, not Cloud Composer)
- GCS for storage, BigQuery for the warehouse (GCP account already set up)
- GitHub Actions for CI/CD
- pytest for unit tests

## Repo conventions
- Follow the same bronze/silver/gold layer separation and file naming style
  as the cafe/bakery project (`bronze_to_silver.py`, `silver_to_gold.py`,
  `load_to_bigquery.py`)
- Partition landing/bronze data by date: `landing/<YYYY-MM-DD>/`
- Airflow DAG must be idempotent per date partition — rerunning a date
  overwrites that partition's output rather than appending
- No hardcoded "today" — batch jobs should always take a date parameter so
  backfills work
- Every new transform function needs a corresponding pytest unit test before
  it's considered done — this is the CI/CD story, not optional polish

## What NOT to do
- Don't introduce Kafka/Pub-Sub/any broker — this project deliberately uses
  file-based streaming instead
- Don't build a dashboard (Looker Studio etc.) — out of scope, same call as
  the cafe/bakery project
- Don't default to pandas for the layer transforms — PySpark throughout,
  to match the existing portfolio style