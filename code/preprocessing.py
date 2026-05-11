"""
Phase 1 — Data Preprocessing (PySpark)

Loads the Amazon Video Games 5-core JSON dataset, cleans and transforms it,
applies StringIndexer to map string IDs to integer indices, and saves
the cleaned data as Parquet along with the fitted indexer models.

Dataset: Video_Games_5.json from UCSD JMCAULEY
Schema:  reviewerID, asin, overall (1-5), unixReviewTime
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, LongType, DoubleType
)
from pyspark.ml.feature import StringIndexer

# ─── Configuration ──────────────────────────────────────────────────────────────
# Resolve paths relative to the project root (one level up from code/)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "Video_Games_5.json")
CLEANED_PARQUET_PATH = os.path.join(PROJECT_ROOT, "data", "games_cleaned.parquet")
USER_INDEXER_PATH = os.path.join(PROJECT_ROOT, "models", "user_indexer")
ITEM_INDEXER_PATH = os.path.join(PROJECT_ROOT, "models", "item_indexer")


def create_spark_session():
    """Create a local Spark session optimized for preprocessing."""
    return (
        SparkSession.builder
        .appName("GameRec-Preprocessing")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def load_raw_data(spark):
    """
    Load the raw JSON dataset into a Spark DataFrame.
    
    The Amazon 5-core dataset uses one JSON object per line,
    so Spark's default JSON reader handles it natively.
    """
    print(f"\n📂 Loading raw data from: {RAW_DATA_PATH}")
    
    if not os.path.exists(RAW_DATA_PATH):
        print(f"❌ File not found: {RAW_DATA_PATH}")
        print("   Please download the dataset first:")
        print("   URL: https://datarepos.ucsd.edu/dataset/amazon/Video_Games_5.json.gz")
        print(f"   Extract to: {RAW_DATA_PATH}")
        sys.exit(1)
    
    # Define an explicit schema to avoid full-file inference scan
    schema = StructType([
        StructField("reviewerID", StringType(), True),
        StructField("asin", StringType(), True),
        StructField("overall", DoubleType(), True),
        StructField("unixReviewTime", LongType(), True),
        # Additional fields exist (reviewerName, helpful, reviewText, summary)
        # but we only need the four above — Spark will ignore the rest
        StructField("reviewerName", StringType(), True),
        StructField("helpful", StringType(), True),
        StructField("reviewText", StringType(), True),
        StructField("summary", StringType(), True),
    ])
    
    df = spark.read.json(RAW_DATA_PATH, schema=schema)
    print(f"   Raw records loaded: {df.count()}")
    return df


def clean_and_transform(df):
    """
    Select, rename, clean, and deduplicate the DataFrame.
    
    Steps:
    1. Select and rename columns to our target schema
    2. Drop rows with any null in key columns
    3. Filter ratings to valid range [1.0, 5.0]
    4. Remove duplicate (user_id, item_id) pairs, keeping the most recent
    """
    print("\n🔧 Cleaning and transforming data...")
    
    # Step 1: Select and rename
    df_renamed = df.select(
        F.col("reviewerID").alias("user_id"),
        F.col("asin").alias("item_id"),
        F.col("overall").cast(FloatType()).alias("rating"),
        F.col("unixReviewTime").alias("timestamp")
    )
    
    # Step 2: Drop nulls in any key column
    df_no_nulls = df_renamed.dropna(subset=["user_id", "item_id", "rating", "timestamp"])
    null_dropped = df_renamed.count() - df_no_nulls.count()
    print(f"   Dropped {null_dropped} rows with nulls")
    
    # Step 3: Filter ratings to valid range [1.0, 5.0]
    df_valid_ratings = df_no_nulls.filter(
        (F.col("rating") >= 1.0) & (F.col("rating") <= 5.0)
    )
    rating_filtered = df_no_nulls.count() - df_valid_ratings.count()
    print(f"   Filtered {rating_filtered} rows with invalid ratings")
    
    # Step 4: Remove duplicate (user_id, item_id) — keep the most recent review
    # Window partitions by (user_id, item_id) and orders by timestamp descending;
    # we keep only the first row (most recent) per group.
    from pyspark.sql.window import Window
    
    window_spec = Window.partitionBy("user_id", "item_id").orderBy(F.col("timestamp").desc())
    df_deduped = (
        df_valid_ratings
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
    dupes_removed = df_valid_ratings.count() - df_deduped.count()
    print(f"   Removed {dupes_removed} duplicate (user_id, item_id) pairs")
    
    return df_deduped


def apply_string_indexers(df):
    """
    Apply StringIndexer to convert user_id and item_id strings to integer indices.
    
    StringIndexer assigns indices by frequency (most frequent = 0).
    The fitted models are saved for reuse in the streaming pipeline.
    """
    print("\n🔢 Applying StringIndexer to user_id and item_id...")
    
    # Fit and transform user_id → user_idx
    user_indexer = StringIndexer(inputCol="user_id", outputCol="user_idx", handleInvalid="skip")
    user_indexer_model = user_indexer.fit(df)
    df = user_indexer_model.transform(df)
    
    # Fit and transform item_id → item_idx
    item_indexer = StringIndexer(inputCol="item_id", outputCol="item_idx", handleInvalid="skip")
    item_indexer_model = item_indexer.fit(df)
    df = item_indexer_model.transform(df)
    
    # Cast indexed columns to integer for ALS compatibility
    df = df.withColumn("user_idx", F.col("user_idx").cast("integer"))
    df = df.withColumn("item_idx", F.col("item_idx").cast("integer"))
    
    print(f"   User indices: 0 to {df.agg(F.max('user_idx')).collect()[0][0]}")
    print(f"   Item indices: 0 to {df.agg(F.max('item_idx')).collect()[0][0]}")
    
    return df, user_indexer_model, item_indexer_model


def save_outputs(df, user_indexer_model, item_indexer_model):
    """Save the cleaned DataFrame as Parquet and the indexer models."""
    print("\n💾 Saving outputs...")
    
    # Save cleaned data
    df.write.mode("overwrite").parquet(CLEANED_PARQUET_PATH)
    print(f"   Cleaned data saved to: {CLEANED_PARQUET_PATH}")
    
    # Save indexer models
    user_indexer_model.write().overwrite().save(USER_INDEXER_PATH)
    print(f"   User indexer saved to: {USER_INDEXER_PATH}")
    
    item_indexer_model.write().overwrite().save(ITEM_INDEXER_PATH)
    print(f"   Item indexer saved to: {ITEM_INDEXER_PATH}")


def print_summary(df):
    """Print summary statistics about the cleaned dataset."""
    print("\n" + "=" * 60)
    print("📊 DATASET SUMMARY")
    print("=" * 60)
    
    total_records = df.count()
    unique_users = df.select("user_id").distinct().count()
    unique_items = df.select("item_id").distinct().count()
    
    print(f"   Total records:  {total_records:,}")
    print(f"   Unique users:   {unique_users:,}")
    print(f"   Unique items:   {unique_items:,}")
    print(f"   Sparsity:       {1 - total_records / (unique_users * unique_items):.6f}")
    
    # Rating distribution
    print("\n   Rating Distribution:")
    print("   " + "-" * 40)
    rating_dist = (
        df.groupBy("rating")
        .count()
        .orderBy("rating")
        .collect()
    )
    for row in rating_dist:
        pct = row["count"] / total_records * 100
        bar = "█" * int(pct / 2)
        print(f"   ⭐ {row['rating']:.1f}  →  {row['count']:>7,}  ({pct:5.1f}%)  {bar}")
    
    # Basic stats
    print("\n   Rating Statistics:")
    print("   " + "-" * 40)
    stats = df.select(
        F.mean("rating").alias("mean"),
        F.stddev("rating").alias("std"),
        F.min("rating").alias("min"),
        F.max("rating").alias("max"),
    ).collect()[0]
    print(f"   Mean:   {stats['mean']:.3f}")
    print(f"   Std:    {stats['std']:.3f}")
    print(f"   Min:    {stats['min']:.1f}")
    print(f"   Max:    {stats['max']:.1f}")
    
    print("=" * 60)


def main():
    """Main entry point — orchestrates the full preprocessing pipeline."""
    print("=" * 60)
    print("🎮 GAME RECOMMENDATION SYSTEM — Phase 1: Preprocessing")
    print("=" * 60)
    
    spark = create_spark_session()
    
    try:
        # Load raw data
        raw_df = load_raw_data(spark)
        
        # Clean and transform
        cleaned_df = clean_and_transform(raw_df)
        
        # Apply StringIndexers
        indexed_df, user_idx_model, item_idx_model = apply_string_indexers(cleaned_df)
        
        # Print summary before saving
        print_summary(indexed_df)
        
        # Save all outputs
        save_outputs(indexed_df, user_idx_model, item_idx_model)
        
        print("\n✅ Phase 1 complete! Ready for Phase 2 (ALS Training).")
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
