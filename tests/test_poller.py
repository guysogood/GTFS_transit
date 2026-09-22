from google.transit import gtfs_realtime_pb2

from poller.poll_mbta import (
    mark_day_complete,
    parse_feed,
    partition_date,
    trip_updates_to_records,
    vehicle_positions_to_records,
    write_records,
)

# 2026-01-15 12:00:00 UTC == 07:00 America/New_York
TS = 1768478400


def _feed():
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = TS
    return feed


def test_trip_updates_flattened_per_stop():
    feed = _feed()
    e = feed.entity.add()
    e.id = "1"
    e.trip_update.trip.trip_id = "t1"
    e.trip_update.trip.route_id = "Red"
    stu = e.trip_update.stop_time_update.add()
    stu.stop_id = "place-sstat"
    stu.stop_sequence = 3
    stu.arrival.delay = 120

    records = trip_updates_to_records(parse_feed(feed.SerializeToString()))

    assert len(records) == 1
    assert records[0]["route_id"] == "Red"
    assert records[0]["arrival_delay"] == 120
    assert records[0]["departure_delay"] is None
    assert records[0]["feed_timestamp"] == TS


def test_vehicle_positions_skips_non_vehicle_entities():
    feed = _feed()
    feed.entity.add().id = "empty"
    v = feed.entity.add()
    v.id = "2"
    v.vehicle.vehicle.id = "y1234"
    v.vehicle.trip.route_id = "Orange"
    v.vehicle.position.latitude = 42.35
    v.vehicle.position.longitude = -71.06

    records = vehicle_positions_to_records(feed)

    assert [r["vehicle_id"] for r in records] == ["y1234"]


def test_partition_date_uses_eastern_time():
    assert partition_date(TS) == "2026-01-15"
    # 03:00 UTC on the 16th is still the 15th in Boston
    assert partition_date(TS + 15 * 3600) == "2026-01-15"


def test_write_records_and_success_marker(tmp_path):
    path = write_records(tmp_path, "trip_updates", TS, [{"a": 1}, {"a": 2}])
    assert path.parent.name == "2026-01-15"
    assert len(path.read_text().splitlines()) == 2
    assert not list(path.parent.glob(".*.tmp"))

    mark_day_complete(tmp_path, "2026-01-15")
    assert (path.parent / "_SUCCESS").exists()
