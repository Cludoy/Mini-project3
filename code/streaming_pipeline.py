"""
Phase 4 — Spark Structured Streaming Pipeline

Reads real-time events from Kafka topic "game-events", parses JSON safely
(routing malformed records to a dead-letter sink), applies windowed analytics
(30s window / 10s slide), computes engagement scores, and generates alerts
for trending items and activity spikes.

Watermark Policy:
  - Watermark: 15 seconds on event_time
  - Window: 30 seconds, sliding every 10 seconds
  - Rationale: Data arriving > 15s late is DROPPED. Since the slide interval
    is 10s, data older than 15s cannot meaningfully contribute to any active
    slide. Dropping is preferable to processing stale data that no longer
    reflects real user intent.

Outputs:
  - Console sink for alerts and analytics
  - Memory sink tables for dashboard queries:
    * "window_analytics" — per-window aggregated metrics
    * "dead_letters"     — malformed records
    * "alerts"           — triggered alerts
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, FloatType, StringType, TimestampType
)

# ─── Configuration ──────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_NAME = "game-events"

# Window parameters
WINDOW_DURATION = "30 seconds"
SLIDE_DURATION = "10 seconds"
WATERMARK_DELAY = "15 seconds"

# Alert thresholds
TRENDING_MIN_AVG_RATING = 4.5     # Avg rating above this → trending candidate
TRENDING_MIN_INTERACTIONS = 3     # Must have at least this many interactions
ACTIVITY_SPIKE_THRESHOLD = 10     # User interactions above this → activity spike

# Checkpoint directories (for fault tolerance)
CHECKPOINT_DIR = os.path.join(PROJECT_ROOT, "checkpoints")


def create_spark_session():
    """
    Create a Spark session configured for Structured Streaming with Kafka.
    
    The kafka package is loaded via spark.jars.packages to handle the
    Kafka data source connector.
    """
    return (
        SparkSession.builder
        .appName("GameRec-StreamingPipeline")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .getOrCreate()
    )


def define_event_schema():
    """
    Define the expected JSON schema for incoming events.
    
    Schema matches the payload produced by kafka_producer.py:
    {
        "user_id": int,
        "item_id": int,
        "rating": float,
        "timestamp": string (ISO8601)
    }
    """
    return StructType([
        StructField("user_id", IntegerType(), True),
        StructField("item_id", IntegerType(), True),
        StructField("rating", FloatType(), True),
        StructField("timestamp", StringType(), True),
    ])


def read_kafka_stream(spark):
    """
    Read from Kafka topic using Spark's readStream.
    
    Each Kafka record has: key, value (JSON bytes), topic, partition, offset, timestamp.
    We extract the value column and cast it to string for JSON parsing.
    """
    print(f"\n📡 Connecting to Kafka topic '{TOPIC_NAME}' at {KAFKA_BOOTSTRAP}...")
    
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    
    # Extract the value as a string (Kafka sends bytes)
    return raw_stream.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_timestamp")


def parse_json_safely(raw_df, schema):
    """
    Parse JSON with safe error handling — route malformed records to dead letter.
    
    Strategy:
    - Use from_json with the expected schema
    - Records that fail to parse will have NULL in the parsed struct
    - We split into two streams: valid records and dead letters
    """
    print("\n🔄 Setting up JSON parsing with dead-letter routing...")
    
    # Parse JSON using the schema — malformed records will have null fields
    parsed_df = raw_df.withColumn(
        "parsed", F.from_json(F.col("json_str"), schema)
    )
    
    # ─── Valid Records ──────────────────────────────────────────────────────
    # A record is valid if all required fields are non-null AND rating is in [1.0, 5.0]
    valid_df = (
        parsed_df
        .filter(
            F.col("parsed.user_id").isNotNull() &
            F.col("parsed.item_id").isNotNull() &
            F.col("parsed.rating").isNotNull() &
            F.col("parsed.timestamp").isNotNull() &
            (F.col("parsed.rating") >= 1.0) &
            (F.col("parsed.rating") <= 5.0)
        )
        .select(
            F.col("parsed.user_id").alias("user_id"),
            F.col("parsed.item_id").alias("item_id"),
            F.col("parsed.rating").alias("rating"),
            # Cast ISO8601 string → TimestampType for watermarking
            F.to_timestamp(F.col("parsed.timestamp")).alias("event_time"),
            F.col("kafka_timestamp"),
        )
    )
    
    # ─── Dead Letters (malformed records) ───────────────────────────────────
    # Records where parsing failed or validation didn't pass
    dead_letters = (
        parsed_df
        .filter(
            F.col("parsed.user_id").isNull() |
            F.col("parsed.item_id").isNull() |
            F.col("parsed.rating").isNull() |
            F.col("parsed.timestamp").isNull() |
            (F.col("parsed.rating") < 1.0) |
            (F.col("parsed.rating") > 5.0)
        )
        .select(
            F.col("json_str").alias("raw_payload"),
            F.current_timestamp().alias("received_at"),
            F.lit("PARSE_ERROR").alias("error_type"),
        )
    )
    
    return valid_df, dead_letters


def apply_window_analytics(valid_df):
    """
    Apply windowed aggregations with watermarking.
    
    Window: 30 seconds, sliding every 10 seconds
    Watermark: 15 seconds (data older than this is dropped)
    
    Computes per window:
    1. avg_rating per item — average rating within the window
    2. interaction_count per user — number of interactions per user
    3. engagement_score — custom metric: (count × avg_rating) / window_duration
       This rewards items that are both frequently rated AND highly rated.
    """
    print("\n📊 Configuring window analytics...")
    print(f"   Window: {WINDOW_DURATION}, Slide: {SLIDE_DURATION}")
    print(f"   Watermark: {WATERMARK_DELAY}")
    
    # Apply watermark for late data handling
    watermarked_df = valid_df.withWatermark("event_time", WATERMARK_DELAY)
    
    # ─── Item-level aggregation ─────────────────────────────────────────────
    item_window_agg = (
        watermarked_df
        .groupBy(
            F.window("event_time", WINDOW_DURATION, SLIDE_DURATION),
            "item_id"
        )
        .agg(
            F.avg("rating").alias("avg_rating"),
            F.count("*").alias("interaction_count"),
            F.min("event_time").alias("window_start_time"),
            F.max("event_time").alias("window_end_time"),
        )
        # Engagement score: (interaction_count × avg_rating) / window_duration_seconds
        # window_duration_seconds = 30 (fixed)
        .withColumn(
            "engagement_score",
            (F.col("interaction_count") * F.col("avg_rating")) / 30.0
        )
    )
    
    # ─── User-level aggregation ─────────────────────────────────────────────
    user_window_agg = (
        watermarked_df
        .groupBy(
            F.window("event_time", WINDOW_DURATION, SLIDE_DURATION),
            "user_id"
        )
        .agg(
            F.count("*").alias("interaction_count"),
            F.avg("rating").alias("avg_rating"),
        )
    )
    
    return item_window_agg, user_window_agg


def process_alerts_batch(batch_df, batch_id, alert_type):
    """
    Process each micro-batch to detect and print alerts.
    
    Called by foreachBatch — evaluates alert conditions on each batch.
    """
    if batch_df.isEmpty():
        return
    
    if alert_type == "trending":
        # ALERT TYPE 1 — Trending Item: avg_rating > 4.5 AND interaction_count > 3
        trending = batch_df.filter(
            (F.col("avg_rating") > TRENDING_MIN_AVG_RATING) &
            (F.col("interaction_count") > TRENDING_MIN_INTERACTIONS)
        )
        
        if trending.count() > 0:
            rows = trending.collect()
            for row in rows:
                print(f"\n🚨 ALERT [Batch {batch_id}]: "
                      f"Item {row['item_id']} is TRENDING | "
                      f"Avg Rating: {row['avg_rating']:.2f} | "
                      f"Interactions: {row['interaction_count']} | "
                      f"Engagement Score: {row['engagement_score']:.3f}")
    
    elif alert_type == "activity_spike":
        # ALERT TYPE 2 — Activity Spike: user interaction_count > 10 in window
        spikes = batch_df.filter(
            F.col("interaction_count") > ACTIVITY_SPIKE_THRESHOLD
        )
        
        if spikes.count() > 0:
            rows = spikes.collect()
            for row in rows:
                print(f"\n⚡ ALERT [Batch {batch_id}]: "
                      f"Activity SPIKE for User {row['user_id']} | "
                      f"Interactions: {row['interaction_count']} | "
                      f"Avg Rating: {row['avg_rating']:.2f}")


def start_streaming(spark, valid_df, dead_letters, item_agg, user_agg):
    """
    Start all streaming queries:
    1. Item analytics → console + memory sink (with trending alerts)
    2. User analytics → console + memory sink (with activity spike alerts)
    3. Dead letters → console sink
    4. Raw valid events → memory sink for dashboard
    """
    print("\n🚀 Starting streaming queries...")
    
    queries = []
    
    # ─── Query 1: Item Window Analytics (with Trending Alerts) ──────────────
    item_query = (
        item_agg
        .writeStream
        .outputMode("update")
        .foreachBatch(lambda df, bid: process_alerts_batch(df, bid, "trending"))
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "item_analytics"))
        .queryName("item_analytics")
        .start()
    )
    queries.append(item_query)
    print("   ✅ Item analytics stream started (trending alerts enabled)")
    
    # ─── Query 2: Item Analytics → Memory Sink for Dashboard ────────────────
    item_memory_query = (
        item_agg
        .writeStream
        .outputMode("update")
        .format("memory")
        .queryName("window_analytics")
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "item_memory"))
        .start()
    )
    queries.append(item_memory_query)
    print("   ✅ Item analytics → memory sink 'window_analytics'")
    
    # ─── Query 3: User Window Analytics (with Activity Spike Alerts) ────────
    user_query = (
        user_agg
        .writeStream
        .outputMode("update")
        .foreachBatch(lambda df, bid: process_alerts_batch(df, bid, "activity_spike"))
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "user_analytics"))
        .queryName("user_analytics")
        .start()
    )
    queries.append(user_query)
    print("   ✅ User analytics stream started (activity spike alerts enabled)")
    
    # ─── Query 4: User Analytics → Memory Sink for Dashboard ────────────────
    user_memory_query = (
        user_agg
        .writeStream
        .outputMode("update")
        .format("memory")
        .queryName("user_activity")
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "user_memory"))
        .start()
    )
    queries.append(user_memory_query)
    print("   ✅ User analytics → memory sink 'user_activity'")
    
    # ─── Query 5: Dead Letters → Console Sink ───────────────────────────────
    dead_letter_query = (
        dead_letters
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "dead_letters"))
        .queryName("dead_letters")
        .start()
    )
    queries.append(dead_letter_query)
    print("   ✅ Dead letter stream started (console sink)")
    
    # ─── Query 6: Raw Valid Events → Memory Sink ────────────────────────────
    raw_events_query = (
        valid_df
        .writeStream
        .outputMode("append")
        .format("memory")
        .queryName("live_events")
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "live_events"))
        .start()
    )
    queries.append(raw_events_query)
    print("   ✅ Live events → memory sink 'live_events'")
    
    return queries


def main():
    """Main entry point — orchestrates the streaming pipeline."""
    print("=" * 60)
    print("🎮 GAME RECOMMENDATION SYSTEM — Phase 4: Streaming Pipeline")
    print("=" * 60)
    
    spark = create_spark_session()
    
    try:
        # Define event schema
        schema = define_event_schema()
        
        # Read from Kafka
        raw_df = read_kafka_stream(spark)
        
        # Parse JSON with dead-letter routing
        valid_df, dead_letters = parse_json_safely(raw_df, schema)
        
        # Apply window analytics
        item_agg, user_agg = apply_window_analytics(valid_df)
        
        # Start all streaming queries
        queries = start_streaming(spark, valid_df, dead_letters, item_agg, user_agg)
        
        print("\n" + "=" * 60)
        print("🟢 All streaming queries active. Waiting for events...")
        print("   Press Ctrl+C to stop.")
        print("=" * 60)
        
        # Wait for any query to terminate (blocks forever unless error)
        spark.streams.awaitAnyTermination()
    
    except KeyboardInterrupt:
        print("\n\n🛑 Streaming pipeline stopped by user.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
