import argparse
import json
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from google.transit import gtfs_realtime_pb2

TZ = ZoneInfo("America/New_York")
FEED_URLS = {
    "trip_updates": "https://cdn.mbta.com/realtime/TripUpdates.pb",
    "vehicle_positions": "https://cdn.mbta.com/realtime/VehiclePositions.pb",
}


def parse_feed(content: bytes) -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(content)
    return feed


def partition_date(feed_timestamp: int) -> str:
    return datetime.fromtimestamp(feed_timestamp, TZ).strftime("%Y-%m-%d")


def trip_updates_to_records(feed: gtfs_realtime_pb2.FeedMessage) -> list[dict]:
    ts = feed.header.timestamp
    records = []
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        for stu in tu.stop_time_update:
            records.append(
                {
                    "feed_timestamp": ts,
                    "trip_id": tu.trip.trip_id,
                    "route_id": tu.trip.route_id,
                    "direction_id": tu.trip.direction_id,
                    "vehicle_id": tu.vehicle.id or None,
                    "stop_id": stu.stop_id,
                    "stop_sequence": stu.stop_sequence,
                    "arrival_delay": stu.arrival.delay if stu.HasField("arrival") else None,
                    "departure_delay": stu.departure.delay if stu.HasField("departure") else None,
                }
            )
    return records


def vehicle_positions_to_records(feed: gtfs_realtime_pb2.FeedMessage) -> list[dict]:
    ts = feed.header.timestamp
    records = []
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        v = entity.vehicle
        records.append(
            {
                "feed_timestamp": ts,
                "vehicle_id": v.vehicle.id,
                "trip_id": v.trip.trip_id,
                "route_id": v.trip.route_id,
                "latitude": v.position.latitude,
                "longitude": v.position.longitude,
                "current_status": v.current_status,
                "stop_id": v.stop_id or None,
            }
        )
    return records


CONVERTERS = {
    "trip_updates": trip_updates_to_records,
    "vehicle_positions": vehicle_positions_to_records,
}


def write_records(landing: Path, feed_type: str, feed_timestamp: int, records: list[dict]) -> Path:
    day_dir = landing / partition_date(feed_timestamp)
    day_dir.mkdir(parents=True, exist_ok=True)
    tmp = day_dir / f".{feed_type}_{feed_timestamp}.tmp"
    final = day_dir / f"{feed_type}_{feed_timestamp}.json"
    tmp.write_text("\n".join(json.dumps(r) for r in records))
    tmp.rename(final)  # atomic, so the streaming reader never sees partial files
    return final


def mark_day_complete(landing: Path, date_str: str) -> None:
    day_dir = landing / date_str
    if day_dir.exists():
        (day_dir / "_SUCCESS").touch()


def poll_once(landing: Path, current_date: str | None) -> str | None:
    """Poll every feed once; returns the partition date seen, writing _SUCCESS on rollover."""
    for feed_type, url in FEED_URLS.items():
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        feed = parse_feed(resp.content)
        records = CONVERTERS[feed_type](feed)
        write_records(landing, feed_type, feed.header.timestamp, records)
        new_date = partition_date(feed.header.timestamp)
        if current_date and new_date > current_date:
            mark_day_complete(landing, current_date)
        current_date = max(filter(None, [current_date, new_date]))
    return current_date


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--landing", default="landing")
    parser.add_argument("--interval", type=int, default=30)
    args = parser.parse_args()
    landing = Path(args.landing)
    current_date = None
    while True:
        try:
            current_date = poll_once(landing, current_date)
        except requests.RequestException as exc:
            print(f"poll failed: {exc}")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
