import pytest

from batch.load_to_bigquery import (
    build_load_config,
    gcs_prefix,
    list_gold_files,
    load,
    partition_table_id,
)


def test_partition_table_id_targets_single_day():
    assert partition_table_id("p.d.route_daily_metrics", "2026-01-15") == "p.d.route_daily_metrics$20260115"


def test_gcs_prefix_is_date_partitioned():
    assert gcs_prefix("2026-01-15") == "gold/route_daily_metrics/date=2026-01-15"


def test_list_gold_files_ignores_markers(tmp_path):
    d = tmp_path / "route_daily_metrics" / "date=2026-01-15"
    d.mkdir(parents=True)
    (d / "part-0.snappy.parquet").touch()
    (d / "_SUCCESS").touch()
    (d / ".part-0.snappy.parquet.crc").touch()

    assert [f.name for f in list_gold_files(tmp_path, "2026-01-15")] == ["part-0.snappy.parquet"]


def test_load_fails_fast_without_gold_data(tmp_path):
    with pytest.raises(FileNotFoundError):
        load("2026-01-15", tmp_path, "p", "d", "b")


def test_load_config_truncates_and_partitions_by_service_date():
    cfg = build_load_config()
    assert cfg.write_disposition == "WRITE_TRUNCATE"
    assert cfg.source_format == "PARQUET"
    assert cfg.time_partitioning.field == "service_date"
