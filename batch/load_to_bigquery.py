import argparse
import os
from pathlib import Path

TABLE_NAME = "route_daily_metrics"


def gcs_prefix(date: str) -> str:
    return f"gold/{TABLE_NAME}/date={date}"


def partition_table_id(table: str, date: str) -> str:
    """Partition decorator: a WRITE_TRUNCATE load to it replaces only that day's partition."""
    return f"{table}${date.replace('-', '')}"


def list_gold_files(gold: Path, date: str) -> list[Path]:
    return sorted((gold / TABLE_NAME / f"date={date}").glob("*.parquet"))


def build_load_config():
    from google.cloud import bigquery

    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="service_date"),
    )


def upload_to_gcs(files: list[Path], bucket_name: str, date: str) -> list[str]:
    from google.cloud import storage

    bucket = storage.Client().bucket(bucket_name)
    uris = []
    for f in files:
        blob = bucket.blob(f"{gcs_prefix(date)}/{f.name}")
        blob.upload_from_filename(str(f))
        uris.append(f"gs://{bucket_name}/{blob.name}")
    return uris


def load(date: str, gold: Path, project: str, dataset: str, bucket: str) -> None:
    from google.cloud import bigquery

    files = list_gold_files(gold, date)
    if not files:
        raise FileNotFoundError(f"no gold parquet files for {date} under {gold}")
    uris = upload_to_gcs(files, bucket, date)
    client = bigquery.Client(project=project)
    table_id = partition_table_id(f"{project}.{dataset}.{TABLE_NAME}", date)
    client.load_table_from_uri(uris, table_id, job_config=build_load_config()).result()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD partition to load")
    parser.add_argument("--gold", default="data/gold")
    parser.add_argument("--project", default=os.environ.get("GCP_PROJECT"))
    parser.add_argument("--dataset", default=os.environ.get("BQ_DATASET"))
    parser.add_argument("--bucket", default=os.environ.get("GCS_BUCKET"))
    args = parser.parse_args()
    missing = [n for n in ("project", "dataset", "bucket") if not getattr(args, n)]
    if missing:
        parser.error(f"missing {missing}; pass flags or set GCP_PROJECT / BQ_DATASET / GCS_BUCKET")
    load(args.date, Path(args.gold), args.project, args.dataset, args.bucket)


if __name__ == "__main__":
    main()
