from streaming.stream_windowed import TRIP_UPDATES_SCHEMA, make_batch_writer, windowed_avg_delay

# 2026-01-15 12:00:00 UTC, aligned to a 5-minute boundary
T0 = 1768478400


def _row(ts, route, arr=None, dep=None):
    return (ts, "t", route, 0, "v", "s", 1, arr, dep)


def test_avg_delay_per_route_per_window(spark):
    df = spark.createDataFrame(
        [
            _row(T0 + 10, "Red", arr=60),
            _row(T0 + 20, "Red", arr=180),
            _row(T0 + 30, "Orange", dep=30),  # falls back to departure delay
            _row(T0 + 310, "Red", arr=600),  # next window
            _row(T0 + 40, "Red"),  # no delay -> dropped
        ],
        TRIP_UPDATES_SCHEMA,
    )

    out = {
        (r.window_start.timestamp(), r.route_id): (r.avg_delay_sec, r.n_predictions)
        for r in windowed_avg_delay(df).collect()
    }

    assert out == {
        (T0, "Red"): (120.0, 2),
        (T0, "Orange"): (30.0, 1),
        (T0 + 300, "Red"): (600.0, 1),
    }


def test_batch_writer_overwrites_only_touched_partitions(spark, tmp_path):
    write = make_batch_writer(str(tmp_path))
    first = spark.createDataFrame(
        [
            ("2026-01-15 07:00:00", "2026-01-15 07:05:00", "Red", 60.0, 1),
            ("2026-01-15 07:00:00", "2026-01-15 07:05:00", "Orange", 30.0, 1),
        ],
        "window_start string, window_end string, route_id string, avg_delay_sec double, n_predictions long",
    ).selectExpr("cast(window_start as timestamp) window_start", "cast(window_end as timestamp) window_end",
                 "route_id", "avg_delay_sec", "n_predictions")
    update = first.where("route_id = 'Red'").selectExpr(
        "window_start", "window_end", "route_id", "cast(120.0 as double) as avg_delay_sec", "2L as n_predictions"
    )

    write(first, 0)
    write(update, 1)

    rows = {r.route_id: (r.avg_delay_sec, r.n_predictions) for r in spark.read.parquet(str(tmp_path)).collect()}
    assert rows == {"Red": (120.0, 2), "Orange": (30.0, 1)}
