import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from streaming.stream_windowed import TRIP_UPDATES_SCHEMA


def clean_trip_updates(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("event_time", F.col("feed_timestamp").cast("timestamp"))
        .withColumn("delay_sec", F.coalesce("arrival_delay", "departure_delay"))
        .where(
            F.col("trip_id").isNotNull()
            & F.col("route_id").isNotNull()
            & F.col("stop_id").isNotNull()
            & F.col("delay_sec").isNotNull()
        )
        .dropDuplicates(["trip_id", "stop_id", "feed_timestamp"])
        .select(
            "event_time",
            "trip_id",
            "route_id",
            "direction_id",
            "vehicle_id",
            "stop_id",
            "stop_sequence",
            "delay_sec",
        )
    )


def run(spark: SparkSession, date: str, landing: str, silver: str) -> None:
    raw = spark.read.schema(TRIP_UPDATES_SCHEMA).json(f"{landing}/{date}/trip_updates_*.json")
    clean_trip_updates(raw).write.mode("overwrite").parquet(f"{silver}/trip_updates/date={date}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD partition to process")
    parser.add_argument("--landing", default="landing")
    parser.add_argument("--silver", default="data/silver")
    args = parser.parse_args()
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("bronze_to_silver")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    run(spark, args.date, args.landing, args.silver)


if __name__ == "__main__":
    main()
