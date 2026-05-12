"""
Phase 5 — Spark Structured Streaming Pipeline

Reads from Kafka "game-events", parses JSON safely (dead-letter routing),
applies windowed analytics (30s/10s), computes engagement scores, and
generates trending/spike alerts.

Watermark: 15s — data older than this is dropped.
Rationale: 30s window with 10s slide → late data > 15s cannot fill a complete
slide interval and would produce partial aggregates that skew scores.

Memory sinks for dashboard: window_metrics, alert_feed
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

CHECKPOINT_DIR = os.path.join(PROJECT_ROOT, "checkpoints")


def create_spark():
    return (
        SparkSession.builder
        .appName("ProjectNexus-Streaming")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )


def main():
    print("=" * 55)
    print("[PROJECT NEXUS — Phase 5: Streaming Pipeline]")
    print("=" * 55)

    spark = create_spark()

    # Event schema matching kafka_producer.py
    schema = StructType([
        StructField("user_id", IntegerType()),
        StructField("item_id", IntegerType()),
        StructField("rating", FloatType()),
        StructField("timestamp", StringType()),
    ])

    # ── Read from Kafka ──────────────────────────────────────────────
    print("\n[Subscribing to game-events @ localhost:9092]")
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "game-events")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse JSON safely ────────────────────────────────────────────
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), schema).alias("data"),
        F.col("value").cast("string").alias("raw_value"),
    )

    # Valid records: all required fields present + valid rating
    valid = (
        parsed
        .filter(
            F.col("data.user_id").isNotNull()
            & F.col("data.item_id").isNotNull()
            & F.col("data.rating").isNotNull()
            & F.col("data.timestamp").isNotNull()
            & (F.col("data.rating") >= 1.0)
            & (F.col("data.rating") <= 5.0)
        )
        .select(
            F.col("data.user_id").alias("user_id"),
            F.col("data.item_id").alias("item_id"),
            F.col("data.rating").alias("rating"),
            F.to_timestamp(F.col("data.timestamp")).alias("event_time"),
        )
    )

    # Dead letters: malformed records
    invalid = (
        parsed
        .filter(
            F.col("data.user_id").isNull()
            | F.col("data.rating").isNull()
            | (F.col("data.rating") < 1.0)
            | (F.col("data.rating") > 5.0)
        )
        .select(
            F.col("raw_value").alias("payload"),
            F.current_timestamp().alias("received_at"),
        )
    )

    # ── Watermark ────────────────────────────────────────────────────
    watermarked = valid.withWatermark("event_time", "15 seconds")

    # ── Window Analytics ─────────────────────────────────────────────
    windowed = (
        watermarked
        .groupBy(F.window("event_time", "30 seconds", "10 seconds"), "item_id")
        .agg(
            F.avg("rating").alias("avg_rating"),
            F.count("*").alias("interaction_count"),
        )
        .withColumn(
            "engagement_score",
            F.round(F.col("interaction_count") * F.col("avg_rating") / 30.0, 4),
        )
    )

    # ── Alerts ───────────────────────────────────────────────────────
    # Trending: avg_rating > 4.5 AND interaction_count > 3
    alerts = (
        windowed
        .filter((F.col("avg_rating") > 4.5) & (F.col("interaction_count") > 3))
        .select(
            F.lit("TRENDING").alias("alert_type"),
            F.col("item_id"),
            F.col("avg_rating"),
            F.col("engagement_score"),
            F.current_timestamp().alias("alert_time"),
        )
    )

    # User-level aggregation for activity spike detection
    user_agg = (
        watermarked
        .groupBy(F.window("event_time", "30 seconds", "10 seconds"), "user_id")
        .agg(F.count("*").alias("interaction_count"))
    )

    user_alerts = (
        user_agg
        .filter(F.col("interaction_count") > 10)
        .select(
            F.lit("ACTIVITY_SPIKE").alias("alert_type"),
            F.col("user_id").alias("item_id"),  # reuse column name for union
            F.lit(0.0).cast("double").alias("avg_rating"),
            F.col("interaction_count").cast("double").alias("engagement_score"),
            F.current_timestamp().alias("alert_time"),
        )
    )

    all_alerts = alerts.union(user_alerts)

    # ── Output Sinks ─────────────────────────────────────────────────
    print("\n[Starting streaming queries...]")

    queries = []

    # 1. Window metrics → memory (dashboard reads this)
    queries.append(
        windowed.writeStream
        .format("memory").queryName("window_metrics")
        .outputMode("update")
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "window_metrics"))
        .start()
    )
    print("   [OK] window_metrics → memory sink")

    # 2. Alerts → memory
    queries.append(
        all_alerts.writeStream
        .format("memory").queryName("alert_feed")
        .outputMode("update")
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "alert_feed"))
        .start()
    )
    print("   [OK] alert_feed → memory sink")

    # 3. Console output (demo)
    queries.append(
        windowed.writeStream
        .format("console").outputMode("update")
        .option("truncate", "false")
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "console"))
        .queryName("console_out")
        .start()
    )
    print("   [OK] console output")

    # 4. Dead letters → console
    queries.append(
        invalid.writeStream
        .format("console").outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "dead_letters"))
        .queryName("dead_letter")
        .start()
    )
    print("   [OK] dead_letter → console")

    # 5. Raw valid events → memory (for dashboard live feed)
    queries.append(
        valid.writeStream
        .format("memory").queryName("live_events")
        .outputMode("append")
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "live_events"))
        .start()
    )
    print("   [OK] live_events → memory sink")

    print(f"\n[All {len(queries)} queries active. Ctrl+C to stop.]")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
