import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

TRIP_UPDATES_SCHEMA = StructType(
    [
        StructField("feed_timestamp", LongType()),
        StructField("trip_id", StringType()),
        StructField("route_id", StringType()),
        StructField("direction_id", IntegerType()),
        StructField("vehicle_id", StringType()),
        StructField("stop_id", StringType()),
        StructField("stop_sequence", IntegerType()),
        StructField("arrival_delay", IntegerType()),
        StructField("departure_delay", IntegerType()),
    ]
)


def windowed_avg_delay(df: DataFrame, window: str = "5 minutes", watermark: str = "2 minutes") -> DataFrame:
    """Average predicted delay per route per event-time window (works on batch and streaming frames)."""
    return (
        df.withColumn("event_time", F.col("feed_timestamp").cast("timestamp"))
        .withColumn("delay_sec", F.coalesce("arrival_delay", "departure_delay"))
        .where(F.col("delay_sec").isNotNull() & F.col("route_id").isNotNull())
        .withWatermark("event_time", watermark)
        .groupBy(F.window("event_time", window).alias("w"), "route_id")
        .agg(
            F.avg("delay_sec").alias("avg_delay_sec"),
            F.count("*").alias("n_predictions"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "route_id",
            "avg_delay_sec",
            "n_predictions",
        )
    )


def make_batch_writer(hot_path: str):
    """Update mode emits the full current row for every changed (window, route); overwrite just those partitions."""

    def write_batch(batch_df: DataFrame, _batch_id: int) -> None:
        (
            batch_df.write.mode("overwrite")
            .option("partitionOverwriteMode", "dynamic")
            .partitionBy("window_start", "route_id")
            .parquet(hot_path)
        )

    return write_batch


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--landing", default="landing")
    parser.add_argument("--hot", default="hot/route_delay")
    parser.add_argument("--checkpoint", default="checkpoints/route_delay")
    parser.add_argument("--trigger-seconds", type=int, default=30)
    args = parser.parse_args()

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("gtfs_stream_route_delay")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    raw = spark.readStream.schema(TRIP_UPDATES_SCHEMA).json(f"{args.landing}/*/trip_updates_*.json")
    query = (
        windowed_avg_delay(raw)
        .writeStream.outputMode("update")
        .foreachBatch(make_batch_writer(args.hot))
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
