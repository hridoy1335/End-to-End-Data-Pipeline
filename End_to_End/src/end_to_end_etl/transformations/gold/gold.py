from pyspark import pipelines as dp
from pyspark.sql.functions import *


@dp.materialized_view(
    comment="Daily aggregated taxi trip metrics for business analytics",
    cluster_by=["trip_date"]
)
def daily_trip_metrics():
    """
    Gold layer: Aggregates daily taxi trip statistics from silver layer.
    Provides business-ready metrics for reporting and analytics.
    """
    return (
        spark.read.table("silver")
        .withColumn("trip_date", to_date(col("tpep_pickup_datetime")))
        .groupBy("trip_date")
        .agg(
            count("*").alias("total_trips"),
            round(avg("fare_amount"), 2).alias("avg_fare"),
            round(avg("trip_distance"), 2).alias("avg_distance"),
            round(sum("fare_amount"), 2).alias("total_revenue"),
            min("fare_amount").alias("min_fare"),
            max("fare_amount").alias("max_fare"),
            countDistinct("pickup_zip").alias("unique_pickup_zones"),
            countDistinct("dropoff_zip").alias("unique_dropoff_zones")
        )
        .orderBy("trip_date")
    )


@dp.materialized_view(
    comment="Top pickup and dropoff zones by trip volume",
    cluster_by=["pickup_zip"]
)
def zone_trip_summary():
    """
    Gold layer: Aggregates trip statistics by pickup and dropoff zones.
    Identifies high-traffic areas for operational planning.
    """
    return (
        spark.read.table("silver")
        .groupBy("pickup_zip", "dropoff_zip")
        .agg(
            count("*").alias("trip_count"),
            round(avg("fare_amount"), 2).alias("avg_fare"),
            round(avg("trip_distance"), 2).alias("avg_distance"),
            round(sum("fare_amount"), 2).alias("total_revenue")
        )
        .filter(col("trip_count") >= 10)  # Filter out low-volume routes
        .orderBy(desc("trip_count"))
    )
