"""
User Segmentation (KMeans)

Extracts user latent factors from the trained ALS model, clusters them
with KMeans (k=5), and precomputes segment-level Top-5 items for cold-start.

Outputs:
  data/user_segments.parquet   — user_idx → segment mapping
  data/segment_top5.parquet    — precomputed Top-5 items per segment
  models/kmeans_model/         — fitted KMeans model
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

CLEANED_PATH = os.path.join(PROJECT_ROOT, "data", "games_cleaned.parquet")
USER_FACTORS_PATH = os.path.join(PROJECT_ROOT, "models", "als_model", "als_user_factors.parquet")
SEGMENTS_PATH = os.path.join(PROJECT_ROOT, "models", "kmeans_model", "user_segments.parquet")
TOP5_PATH = os.path.join(PROJECT_ROOT, "models", "kmeans_model", "segment_top5.parquet")

SEED = 42

# Descriptive labels assigned after analyzing cluster rating behavior
SEGMENT_LABELS = {
    0: "Enthusiast",  # High avg rating, many reviews
    1: "Critic",      # Lower avg rating, selective
    2: "Casual",      # Few reviews, recent activity only
    3: "Explorer",    # Broad genre diversity
    4: "Hardcore",    # High frequency, action-heavy
}


def create_spark():
    return (
        SparkSession.builder
        .appName("ProjectNexus-KMeans")
        .master("local[1]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.shuffle.sort.bypassMergeThreshold", "0")
        .config("spark.local.dir", os.path.join(PROJECT_ROOT, "temp_spark"))
        .getOrCreate()
    )


def main():
    print("=" * 55)
    print("[PROJECT NEXUS — User Segmentation]")
    print("=" * 55)

    spark = create_spark()
    try:
        # ── Load ALS model & extract user factors ─────────────────────
        print(f"\n[Loading ALS user factors from {USER_FACTORS_PATH}]")
        user_factors = spark.read.parquet(USER_FACTORS_PATH)
        user_factors = user_factors.withColumnRenamed("id", "user_idx")

        # userFactors: (user_idx: int, features: array<float>)
        from pyspark.ml.functions import array_to_vector as ml_array_to_vector
        # Native JVM function completely bypasses the Python worker UDF crash
        user_factors = user_factors.withColumn(
            "features_vec", ml_array_to_vector(F.col("features"))
        )
        print(f"   User factors: {user_factors.count():,} users")

        # ── KMeans clustering ─────────────────────────────────────────
        print("\n[Running KMeans (k=5)...]")
        kmeans = KMeans(
            k=5, seed=SEED,
            featuresCol="features_vec",
            predictionCol="segment",
        )
        kmeans_model = kmeans.fit(user_factors)
        
        segmented = kmeans_model.transform(user_factors)

        # Build label map
        label_map = F.create_map(
            *[x for k, v in SEGMENT_LABELS.items() for x in (F.lit(k), F.lit(v))]
        )

        user_segments = segmented.select(
            F.col("user_idx"),
            F.col("segment"),
            label_map[F.col("segment")].alias("segment_label"),
        )

        # Print distribution
        print("\n   Segment Distribution:")
        total = user_segments.count()
        for row in user_segments.groupBy("segment", "segment_label").count().orderBy("segment").collect():
            pct = row["count"] / total * 100
            bar = "█" * int(pct / 2)
            print(f"   [{row['segment']}] {row['segment_label']:<12} → {row['count']:>7,} ({pct:5.1f}%) {bar}")

        # Save via Pandas
        user_segments.toPandas().to_parquet(SEGMENTS_PATH, engine="pyarrow", index=False)
        print(f"\n[User segments saved] → {SEGMENTS_PATH}")

        # ── Precompute Segment Top-5 ─────────────────────────────────
        print("\n[Precomputing segment Top-5 items...]")
        df = spark.read.parquet(CLEANED_PATH)
        df_seg = df.join(user_segments.select("user_idx", "segment"), on="user_idx")

        seg_items = (
            df_seg.groupBy("segment", "item_idx")
            .agg(
                F.avg("rating").alias("avg_rating"),
                F.count("*").alias("n_ratings"),
            )
            .filter(F.col("n_ratings") >= 10)
        )

        w = Window.partitionBy("segment").orderBy(F.col("avg_rating").desc())
        seg_top5 = (
            seg_items
            .withColumn("rank", F.row_number().over(w))
            .filter(F.col("rank") <= 5)
        )

        for seg_id, label in SEGMENT_LABELS.items():
            print(f"\n   [{seg_id}] {label}:")
            rows = seg_top5.filter(F.col("segment") == seg_id).orderBy("rank").collect()
            for r in rows:
                print(f"       #{r['rank']}  item_idx={r['item_idx']}  "
                      f"avg={r['avg_rating']:.2f}  ({r['n_ratings']} reviews)")

        # Save via Pandas
        seg_top5.toPandas().to_parquet(TOP5_PATH, engine="pyarrow", index=False)
        print(f"\n[Segment Top-5 saved] → {TOP5_PATH}")
        print("[Segmentation complete.]")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
