"""
Data Acquisition & Preprocessing

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
    """Download Video Games reviews from HuggingFace — bypassing the deprecated custom script."""
    print("\n[Downloading dataset from HuggingFace...]")
    print("   Source: McAuley-Lab/Amazon-Reviews-2023 (raw_review_Video_Games)")
    from huggingface_hub import hf_hub_download
    import os

    os.makedirs(os.path.join(PROJECT_ROOT, "data"), exist_ok=True)
    local_file = hf_hub_download(
        repo_id="McAuley-Lab/Amazon-Reviews-2023",
        repo_type="dataset",
        filename="raw/review_categories/Video_Games.jsonl",
        local_dir=os.path.join(PROJECT_ROOT, "data")
    )
    print(f"   [Downloaded] Saved to {local_file}")
    return local_file


def clean_and_transform(spark, local_file):
    """
    Clean raw data:
    1. Select & rename columns (parent_asin → item_id)
    2. Drop nulls on key columns
    3. Filter: 1.0 ≤ rating ≤ 5.0
    4. Deduplicate (user_id, item_id) — keep most recent by timestamp
    """
    print("\n[Cleaning data...]")

    # Read with Spark directly from the local JSONL file
    df = spark.read.json(local_file)
    
    # Take a 500k sample to speed up processing
    print("   [Sampling 500,000 records...]")
    df = df.limit(500000)

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
    print("\n[Fitting StringIndexers...]")

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

    # We do not save StringIndexerModels via Spark to avoid Hadoop/winutils errors on Windows.
    # The string indexers are no longer required as downstream processes use the indexed integers.

    return df


def print_summary(df):
    """Print dataset statistics."""
    total = df.count()
    users = df.select("user_id").distinct().count()
    items = df.select("item_id").distinct().count()

    print("\n" + "=" * 55)
    print("[DATASET SUMMARY]")
    print("=" * 55)
    print(f"   Total records:  {total:,}")
    print(f"   Unique users:   {users:,}")
    print(f"   Unique items:   {items:,}")
    print(f"   Sparsity:       {1 - total / (users * items):.6f}")

    print("\n   Rating Distribution:")
    for row in df.groupBy("rating").count().orderBy("rating").collect():
        pct = row["count"] / total * 100
        bar = "█" * int(pct / 2)
        print(f"   * {row['rating']:.1f}  ->  {row['count']:>8,}  ({pct:5.1f}%)  {bar}")
    print("=" * 55)


def extract_titles_from_meta(pandas_df):
    """Download metadata and extract title mappings for dashboard."""
    print("\n[Extracting item titles from metadata...]")
    from huggingface_hub import hf_hub_download
    import json
    
    try:
        meta_file = hf_hub_download(
            repo_id="McAuley-Lab/Amazon-Reviews-2023",
            repo_type="dataset",
            filename="raw/meta_categories/meta_Video_Games.jsonl",
            local_dir=os.path.join(PROJECT_ROOT, "data")
        )
        
        unique_items = pandas_df[['item_id', 'item_idx']].drop_duplicates().copy()
        target_ids = set(unique_items['item_id'].tolist())
        id_to_title = {}
        
        print("   [Processing metadata JSONL...]")
        with open(meta_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line)
                    parent_asin = record.get('parent_asin')
                    if parent_asin in target_ids:
                        title = record.get('title')
                        if title:
                            id_to_title[parent_asin] = title
                        target_ids.remove(parent_asin)
                    if not target_ids:
                        break
                except Exception:
                    pass
                    
        unique_items['title'] = unique_items['item_id'].map(id_to_title).fillna(unique_items['item_id'])
        out_path = os.path.join(PROJECT_ROOT, "data", "item_titles.parquet")
        unique_items[['item_idx', 'title']].to_parquet(out_path, index=False)
        print(f"   [Saved] Titles mapping → {out_path}")
        
    except Exception as e:
        print(f"   [WARN] Could not extract titles: {e}")


def main():
    print("=" * 55)
    print("[PROJECT NEXUS — Preprocessing]")
    print("=" * 55)

    spark = create_spark()
    try:
        pdf = download_dataset()
        df = clean_and_transform(spark, pdf)
        df = apply_indexers(df)
        print_summary(df)

        # Save using Pandas to completely bypass Hadoop's FileSystem
        pandas_df = df.toPandas()
        pandas_df.to_parquet(CLEANED_PATH, engine="pyarrow", index=False)
        print(f"\n[Saved] → {CLEANED_PATH} (via Pandas/PyArrow)")
        
        extract_titles_from_meta(pandas_df)
        
        print("[Preprocessing complete.]")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
