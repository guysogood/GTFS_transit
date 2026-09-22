import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LATE_THRESHOLD_SEC = 300


def route_daily_metrics(df: DataFrame, date: str) -> DataFrame:
    return (
        df.groupBy("route_id")
        .agg(
            F.avg("delay_sec").alias("avg_delay_sec"),
            F.max("delay_sec").alias("max_delay_sec"),
            F.percentile_approx("delay_sec", 0.9).alias("p90_delay_sec"),
            F.avg((F.col("delay_sec") > LATE_THRESHOLD_SEC).cast("double")).alias("pct_late"),
            F.countDistinct("trip_id").alias("n_trips"),
            F.count("*").alias("n_predictions"),
        )
        .withColumn("service_date", F.to_date(F.lit(date)))
    )


def run(spark: SparkSession, date: str, silver: str, gold: str) -> None:
    df = spark.read.parquet(f"{silver}/trip_updates/date={date}")
    route_daily_metrics(df, date).write.mode("overwrite").parquet(f"{gold}/route_daily_metrics/date={date}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD partition to process")
    parser.add_argument("--silver", default="data/silver")
    parser.add_argument("--gold", default="data/gold")
    args = parser.parse_args()
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("silver_to_gold")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    run(spark, args.date, args.silver, args.gold)


if __name__ == "__main__":
    main()
