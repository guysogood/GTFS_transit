import argparse
import sys

NOT_NULL_COLUMNS = ["route_id", "avg_delay_sec", "n_predictions", "service_date"]


def build_stats_query(table: str, columns: list[str]) -> str:
    null_counts = ", ".join(f"COUNTIF({c} IS NULL) AS null_{c}" for c in columns)
    return f"SELECT COUNT(*) AS row_count, {null_counts} FROM `{table}` WHERE service_date = @service_date"


def evaluate_quality(stats: dict, expected_rows: int, columns: list[str]) -> list[str]:
    """Compare loaded-table stats against the gold row count; return a list of problems."""
    errors = []
    if stats["row_count"] == 0:
        errors.append("loaded table has 0 rows for this date")
    elif stats["row_count"] != expected_rows:
        errors.append(f"row count mismatch: loaded {stats['row_count']}, gold has {expected_rows}")
    for c in columns:
        if stats[f"null_{c}"]:
            errors.append(f"{stats[f'null_{c}']} null values in {c}")
    return errors


def fetch_bq_stats(table: str, date: str, columns: list[str]) -> dict:
    from google.cloud import bigquery

    client = bigquery.Client()
    job = client.query(
        build_stats_query(table, columns),
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("service_date", "DATE", date)]
        ),
    )
    return dict(next(iter(job.result())))


def main() -> None:
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--table", required=True, help="project.dataset.table")
    parser.add_argument("--gold", default="data/gold")
    args = parser.parse_args()

    spark = SparkSession.builder.master("local[*]").appName("quality_check").getOrCreate()
    expected = spark.read.parquet(f"{args.gold}/route_daily_metrics/date={args.date}").count()
    errors = evaluate_quality(fetch_bq_stats(args.table, args.date, NOT_NULL_COLUMNS), expected, NOT_NULL_COLUMNS)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
