"""
Phase 1 — Data Acquisition & Preprocessing

Downloads Amazon Video Games reviews from HuggingFace (McAuley-Lab/Amazon-Reviews-2023),
cleans the data, applies StringIndexer mappings, and saves everything needed for downstream
model training.

Outputs:
  data/games_cleaned.parquet   — cleaned & indexed DataFrame
  models/user_indexer/         — fitted StringIndexer (user_id → user_idx)
  models/item_indexer/         — fitted StringIndexer (item_id → item_idx)
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer

# ─── Paths ───────────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CLEANED_PATH = os.path.join(PROJECT_ROOT, "data", "games_cleaned.parquet")
USER_INDEXER_PATH = os.path.join(PROJECT_ROOT, "models", "user_indexer")
ITEM_INDEXER_PATH = os.path.join(PROJECT_ROOT, "models", "item_indexer")


def create_spark():
    return (
        SparkSession.builder
        .appName("ProjectNexus-Preprocessing")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def download_dataset():
    """Download Video Games reviews from HuggingFace — no account required."""
    print("\n📥 Downloading dataset from HuggingFace...")
    print("   McAuley-Lab/Amazon-Reviews-2023, subset: raw_review_Video_Games")
    from datasets import load_dataset

    ds = load_dataset(
        "McAuley-Lab/Amazon-Reviews-2023",
        "raw_review_Video_Games",
        split="full",
        trust_remote_code=True,
    )
    print(f"   Downloaded {len(ds):,} records")
    return ds.to_pandas()


def clean_and_transform(spark, pdf):
    """
    Clean raw data:
    1. Select & rename columns (parent_asin → item_id)
    2. Drop nulls on key columns
    3. Filter: 1.0 ≤ rating ≤ 5.0
    4. Deduplicate (user_id, item_id) — keep most recent by timestamp
    """
    print("\n🔧 Cleaning data...")

    # Convert to Spark DataFrame
    df = spark.createDataFrame(pdf)

    # Select and rename
    df = df.select(
        F.col("user_id"),
        F.col("parent_asin").alias("item_id"),
        F.col("rating").cast(FloatType()),
        F.col("timestamp").alias("timestamp"),
    )

    initial = df.count()

    # Drop nulls
    df = df.dropna(subset=["user_id", "item_id", "rating", "timestamp"])
    after_null = df.count()
    print(f"   Dropped {initial - after_null} null rows")

    # Filter valid ratings
    df = df.filter((F.col("rating") >= 1.0) & (F.col("rating") <= 5.0))
    after_filter = df.count()
    print(f"   Filtered {after_null - after_filter} out-of-range ratings")

    # Deduplicate: keep most recent (user_id, item_id) pair
    w = Window.partitionBy("user_id", "item_id").orderBy(F.col("timestamp").desc())
    df = (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    after_dedup = df.count()
    print(f"   Removed {after_filter - after_dedup} duplicate (user, item) pairs")

    return df


def apply_indexers(df):
    """
    Map string IDs to integer indices using StringIndexer.
    Save fitted models so the streaming pipeline can reuse them.
    """
    print("\n🔢 Fitting StringIndexers...")

    # User indexer
    user_idx = StringIndexer(inputCol="user_id", outputCol="user_idx", handleInvalid="skip")
    user_model = user_idx.fit(df)
    df = user_model.transform(df)

    # Item indexer
    item_idx = StringIndexer(inputCol="item_id", outputCol="item_idx", handleInvalid="skip")
    item_model = item_idx.fit(df)
    df = item_model.transform(df)

    # Cast to integer (ALS requires IntegerType)
    df = df.withColumn("user_idx", F.col("user_idx").cast("integer"))
    df = df.withColumn("item_idx", F.col("item_idx").cast("integer"))

    # Save models
    user_model.write().overwrite().save(USER_INDEXER_PATH)
    print(f"   ✅ User indexer saved → {USER_INDEXER_PATH}")
    item_model.write().overwrite().save(ITEM_INDEXER_PATH)
    print(f"   ✅ Item indexer saved → {ITEM_INDEXER_PATH}")

    return df


def print_summary(df):
    """Print dataset statistics."""
    total = df.count()
    users = df.select("user_id").distinct().count()
    items = df.select("item_id").distinct().count()

    print("\n" + "=" * 55)
    print("📊 DATASET SUMMARY")
    print("=" * 55)
    print(f"   Total records:  {total:,}")
    print(f"   Unique users:   {users:,}")
    print(f"   Unique items:   {items:,}")
    print(f"   Sparsity:       {1 - total / (users * items):.6f}")

    print("\n   Rating Distribution:")
    for row in df.groupBy("rating").count().orderBy("rating").collect():
        pct = row["count"] / total * 100
        bar = "█" * int(pct / 2)
        print(f"   ⭐ {row['rating']:.1f}  →  {row['count']:>8,}  ({pct:5.1f}%)  {bar}")
    print("=" * 55)


def main():
    print("=" * 55)
    print("🎮 PROJECT NEXUS — Phase 1: Preprocessing")
    print("=" * 55)

    spark = create_spark()
    try:
        pdf = download_dataset()
        df = clean_and_transform(spark, pdf)
        df = apply_indexers(df)
        print_summary(df)

        df.write.mode("overwrite").parquet(CLEANED_PATH)
        print(f"\n💾 Saved → {CLEANED_PATH}")
        print("✅ Phase 1 complete.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
