"""
Phase 5 — ML + Streaming Integration: Recommendation Engine

Integrates the trained ALS model with the Spark Structured Streaming pipeline
to generate real-time personalized recommendations.

Two recommendation paths:
  PATH A (Known User): Generate Top-5 via ALS model.recommendForUserSubset()
          → Personalize with user's segment label
  PATH B (Cold-Start):  user_idx == -1 or unknown
          → Determine nearest segment from streaming behavior
          → Serve precomputed segment Top-5 items
          → Label as "Trending in your category"

Performance target: < 5 seconds per recommendation cycle.
"""

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, FloatType, StringType,
    TimestampType, ArrayType
)
from pyspark.ml.recommendation import ALSModel

# ─── Configuration ──────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_NAME = "game-events"

ALS_MODEL_PATH = os.path.join(PROJECT_ROOT, "models", "als_model")
USER_SEGMENTS_PATH = os.path.join(PROJECT_ROOT, "data", "user_segments.parquet")
SEGMENT_TOP5_PATH = os.path.join(PROJECT_ROOT, "data", "segment_top5.parquet")

CHECKPOINT_DIR = os.path.join(PROJECT_ROOT, "checkpoints", "recommendation_engine")

# Segment labels (must match train_als.py)
SEGMENT_LABELS = {
    0: "Casual",
    1: "Enthusiast",
    2: "Critic",
    3: "Hardcore",
    4: "Explorer",
}

# Window / Watermark (same as streaming_pipeline.py)
WINDOW_DURATION = "30 seconds"
SLIDE_DURATION = "10 seconds"
WATERMARK_DELAY = "15 seconds"


def create_spark_session():
    """Create a Spark session with Kafka support."""
    return (
        SparkSession.builder
        .appName("GameRec-RecommendationEngine")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )


def load_models_and_data(spark):
    """
    Load all required models and precomputed data at startup.
    
    Returns:
        als_model: Trained ALS model for known-user recommendations
        user_segments_df: DataFrame mapping user_idx → segment + segment_label
        segment_top5_df: Precomputed Top-5 items per segment for cold-start
        known_user_set: Set of user_idx values that exist in the ALS model
    """
    print("\n📦 Loading models and precomputed data...")
    
    # Load ALS model
    als_model = ALSModel.load(ALS_MODEL_PATH)
    print(f"   ✅ ALS model loaded from: {ALS_MODEL_PATH}")
    print(f"      Rank: {als_model.rank}")
    
    # Load user segments
    user_segments_df = spark.read.parquet(USER_SEGMENTS_PATH)
    print(f"   ✅ User segments loaded: {user_segments_df.count():,} users")
    
    # Load segment Top-5
    segment_top5_df = spark.read.parquet(SEGMENT_TOP5_PATH)
    print(f"   ✅ Segment Top-5 loaded: {segment_top5_df.count()} entries")
    
    # Build set of known user indices for fast lookup
    known_users = set(
        row["user_idx"] for row in 
        user_segments_df.select("user_idx").distinct().collect()
    )
    print(f"   ✅ Known user set built: {len(known_users):,} users")
    
    return als_model, user_segments_df, segment_top5_df, known_users


def recommend_for_known_user(als_model, user_segments_df, user_idx, spark):
    """
    PATH A: Generate Top-5 recommendations for a known user.
    
    Uses model.recommendForUserSubset() to get personalized predictions,
    then annotates with the user's segment label.
    
    Returns a list of dicts: [{item_id, predicted_rating, segment_label}, ...]
    """
    # Create a single-row DataFrame with the user
    user_df = spark.createDataFrame([(user_idx,)], ["user_idx"])
    
    # Generate top-5 recommendations
    recs = als_model.recommendForUserSubset(user_df, 5)
    
    if recs.count() == 0:
        return None  # User not in model factors — fall through to cold-start
    
    # Extract recommendations array
    rec_row = recs.collect()[0]
    rec_items = rec_row["recommendations"]
    
    # Get user's segment label
    segment_row = (
        user_segments_df
        .filter(F.col("user_idx") == user_idx)
        .select("segment_label")
        .collect()
    )
    segment_label = segment_row[0]["segment_label"] if segment_row else "Unknown"
    
    results = []
    for item in rec_items:
        results.append({
            "item_id": item["item_idx"],
            "predicted_rating": round(float(item["rating"]), 3),
            "segment_label": segment_label,
            "rec_type": "Personalized (ALS)",
        })
    
    return results


def recommend_for_cold_start(segment_top5_df, items_interacted, spark):
    """
    PATH B: Cold-start recommendation using segment-based fallback.
    
    Strategy: Determine the nearest segment by majority vote.
    We look at which segment's top items overlap most with the items
    the cold-start user has interacted with in the stream. If no overlap,
    default to segment 0 (Casual — the largest segment typically).
    
    Args:
        segment_top5_df: Precomputed Top-5 items per segment
        items_interacted: List of item_idx values the user interacted with
    
    Returns a list of dicts: [{item_id, predicted_rating, segment_label}, ...]
    """
    if not items_interacted:
        # No interaction history — default to segment 0
        best_segment = 0
    else:
        # Count overlapping items per segment
        segment_scores = {}
        top5_data = segment_top5_df.collect()
        
        for row in top5_data:
            seg = row["segment"]
            if row["item_idx"] in items_interacted:
                segment_scores[seg] = segment_scores.get(seg, 0) + 1
        
        if segment_scores:
            # Pick segment with most overlap (majority vote)
            best_segment = max(segment_scores, key=segment_scores.get)
        else:
            best_segment = 0  # Default fallback
    
    segment_label = SEGMENT_LABELS.get(best_segment, f"Segment-{best_segment}")
    
    # Get Top-5 items for the chosen segment
    top_items = (
        segment_top5_df
        .filter(F.col("segment") == best_segment)
        .orderBy("rank")
        .collect()
    )
    
    results = []
    for item in top_items:
        results.append({
            "item_id": item["item_idx"],
            "predicted_rating": round(float(item["avg_rating"]), 3),
            "segment_label": f"Trending in {segment_label}",
            "rec_type": "Cold-Start (Segment Fallback)",
        })
    
    return results


def process_recommendation_batch(batch_df, batch_id, als_model, user_segments_df,
                                  segment_top5_df, known_users, spark):
    """
    Process each micro-batch of streaming events to generate recommendations.
    
    For each unique user in the batch:
    - Known user → ALS recommendations (PATH A)
    - Cold-start → Segment fallback (PATH B)
    
    Measures and logs latency for each recommendation cycle.
    """
    if batch_df.isEmpty():
        return
    
    batch_start = time.time()
    
    # Get unique users in this batch
    users = batch_df.select("user_id").distinct().collect()
    
    print(f"\n{'='*60}")
    print(f"📋 RECOMMENDATION BATCH #{batch_id} | {len(users)} unique users")
    print(f"{'='*60}")
    
    rec_count = 0
    
    for user_row in users:
        user_id = user_row["user_id"]
        user_start = time.time()
        
        if user_id == -1 or user_id not in known_users:
            # ─── PATH B: Cold-Start User ────────────────────────────────
            # Gather items this user interacted with in the current batch
            items = [
                row["item_id"] for row in
                batch_df.filter(F.col("user_id") == user_id)
                .select("item_id").collect()
            ]
            
            recs = recommend_for_cold_start(segment_top5_df, items, spark)
            path = "🆕 COLD-START"
        else:
            # ─── PATH A: Known User ─────────────────────────────────────
            recs = recommend_for_known_user(als_model, user_segments_df, user_id, spark)
            
            if recs is None:
                # Fallback if user exists in segments but not in model factors
                items = [
                    row["item_id"] for row in
                    batch_df.filter(F.col("user_id") == user_id)
                    .select("item_id").collect()
                ]
                recs = recommend_for_cold_start(segment_top5_df, items, spark)
                path = "🔄 FALLBACK"
            else:
                path = "👤 PERSONALIZED"
        
        user_latency = (time.time() - user_start) * 1000  # ms
        
        # Print recommendations
        if recs:
            print(f"\n   {path} | User {user_id} | Latency: {user_latency:.0f}ms")
            print(f"   {'─'*50}")
            for i, rec in enumerate(recs, 1):
                print(f"   #{i}  Item: {rec['item_id']:<8} | "
                      f"Score: {rec['predicted_rating']:.3f} | "
                      f"Segment: {rec['segment_label']} | "
                      f"Type: {rec['rec_type']}")
            rec_count += 1
    
    batch_latency = (time.time() - batch_start) * 1000
    
    # ─── Latency Tracking ───────────────────────────────────────────────────
    print(f"\n   ⏱️  Batch #{batch_id} completed: {rec_count} recommendations in {batch_latency:.0f}ms")
    if batch_latency > 5000:
        print(f"   ⚠️  WARNING: Batch latency {batch_latency:.0f}ms exceeds 5s target!")
    else:
        print(f"   ✅ Within 5s latency target")


def define_event_schema():
    """Event schema matching kafka_producer.py output."""
    return StructType([
        StructField("user_id", IntegerType(), True),
        StructField("item_id", IntegerType(), True),
        StructField("rating", FloatType(), True),
        StructField("timestamp", StringType(), True),
    ])


def main():
    """Main entry point — integrates ML with streaming for real-time recommendations."""
    print("=" * 60)
    print("🎮 GAME RECOMMENDATION SYSTEM — Phase 5: Recommendation Engine")
    print("=" * 60)
    
    spark = create_spark_session()
    
    try:
        # Load all pre-trained models and data
        als_model, user_segments_df, segment_top5_df, known_users = load_models_and_data(spark)
        
        # Read from Kafka
        schema = define_event_schema()
        
        raw_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", TOPIC_NAME)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
            .selectExpr("CAST(value AS STRING) as json_str")
        )
        
        # Parse JSON — filter out malformed records
        parsed_stream = (
            raw_stream
            .withColumn("parsed", F.from_json(F.col("json_str"), schema))
            .filter(
                F.col("parsed.user_id").isNotNull() &
                F.col("parsed.item_id").isNotNull() &
                F.col("parsed.rating").isNotNull() &
                (F.col("parsed.rating") >= 1.0) &
                (F.col("parsed.rating") <= 5.0)
            )
            .select(
                F.col("parsed.user_id").alias("user_id"),
                F.col("parsed.item_id").alias("item_id"),
                F.col("parsed.rating").alias("rating"),
                F.to_timestamp(F.col("parsed.timestamp")).alias("event_time"),
            )
        )
        
        # Start recommendation stream using foreachBatch
        # This allows us to run batch ML operations on each micro-batch
        rec_query = (
            parsed_stream
            .writeStream
            .outputMode("append")
            .foreachBatch(
                lambda df, bid: process_recommendation_batch(
                    df, bid, als_model, user_segments_df,
                    segment_top5_df, known_users, spark
                )
            )
            .option("checkpointLocation", CHECKPOINT_DIR)
            .queryName("recommendations")
            .start()
        )
        
        # Also write to memory sink for dashboard queries
        rec_memory_query = (
            parsed_stream
            .writeStream
            .outputMode("append")
            .format("memory")
            .queryName("live_recommendations")
            .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "memory"))
            .start()
        )
        
        print("\n" + "=" * 60)
        print("🟢 Recommendation engine active. Waiting for events...")
        print("   Press Ctrl+C to stop.")
        print("=" * 60)
        
        spark.streams.awaitAnyTermination()
    
    except KeyboardInterrupt:
        print("\n\n🛑 Recommendation engine stopped by user.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
