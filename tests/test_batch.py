import json

from batch import bronze_to_silver, silver_to_gold
from batch.bronze_to_silver import clean_trip_updates
from batch.silver_to_gold import route_daily_metrics
from streaming.stream_windowed import TRIP_UPDATES_SCHEMA

T0 = 1768478400


def _row(ts=T0, trip="t1", route="Red", stop="s1", arr=None, dep=None):
    return (ts, trip, route, 0, "v", stop, 1, arr, dep)


def test_clean_trip_updates_filters_and_dedupes(spark):
    df = spark.createDataFrame(
        [
            _row(arr=60),
            _row(arr=60),  # exact duplicate (poller restart)
            _row(trip="t2", dep=90),  # falls back to departure delay
            _row(trip="t3"),  # no delay -> dropped
            _row(trip="t4", route=None, arr=10),  # no route -> dropped
        ],
        TRIP_UPDATES_SCHEMA,
    )

    rows = {r.trip_id: r.delay_sec for r in clean_trip_updates(df).collect()}

    assert rows == {"t1": 60, "t2": 90}


def test_route_daily_metrics(spark):
    df = spark.createDataFrame(
        [
            ("Red", "t1", 0),
            ("Red", "t1", 600),
            ("Red", "t2", 300),
            ("Orange", "t3", 100),
        ],
        "route_id string, trip_id string, delay_sec int",
    )

    out = {r.route_id: r for r in route_daily_metrics(df, "2026-01-15").collect()}

    red = out["Red"]
    assert red.avg_delay_sec == 300.0
    assert red.max_delay_sec == 600
    assert red.n_trips == 2
    assert red.n_predictions == 3
    assert abs(red.pct_late - 1 / 3) < 1e-9  # only 600 exceeds the 300s threshold
    assert str(red.service_date) == "2026-01-15"
    assert out["Orange"].pct_late == 0.0


def test_pipeline_rerun_is_idempotent(spark, tmp_path):
    landing, silver, gold = (str(tmp_path / d) for d in ("landing", "silver", "gold"))
    day = tmp_path / "landing" / "2026-01-15"
    day.mkdir(parents=True)
    records = [
        {"feed_timestamp": T0, "trip_id": "t1", "route_id": "Red", "stop_id": "s1", "arrival_delay": 120},
        {"feed_timestamp": T0 + 30, "trip_id": "t2", "route_id": "Red", "stop_id": "s1", "arrival_delay": 240},
    ]
    (day / f"trip_updates_{T0}.json").write_text("\n".join(json.dumps(r) for r in records))

    for _ in range(2):
        bronze_to_silver.run(spark, "2026-01-15", landing, silver)
        silver_to_gold.run(spark, "2026-01-15", silver, gold)

    assert spark.read.parquet(f"{silver}/trip_updates/date=2026-01-15").count() == 2
    gold_rows = spark.read.parquet(f"{gold}/route_daily_metrics/date=2026-01-15").collect()
    assert len(gold_rows) == 1
    assert gold_rows[0].avg_delay_sec == 180.0
