"""
Phase 2 — Batch ML: ALS Training + User Segmentation

Trains an ALS collaborative filtering model on the cleaned game reviews data.
If initial RMSE > 1.5, performs hyperparameter grid search via CrossValidator.
After training, extracts user latent factors and clusters them with KMeans
to produce user segments for personalization.

Outputs:
  models/als_model/          — Best ALS model
  data/user_segments.parquet — user_idx → segment mapping
  data/segment_top5.parquet  — precomputed Top-5 items per segment
"""

import os
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, ArrayType, FloatType
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import VectorAssembler

# ─── Configuration ──────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CLEANED_PARQUET_PATH = os.path.join(PROJECT_ROOT, "data", "games_cleaned.parquet")
ALS_MODEL_PATH = os.path.join(PROJECT_ROOT, "models", "als_model")
USER_SEGMENTS_PATH = os.path.join(PROJECT_ROOT, "data", "user_segments.parquet")
SEGMENT_TOP5_PATH = os.path.join(PROJECT_ROOT, "data", "segment_top5.parquet")

# Segment descriptive labels based on expected rating behavior patterns
SEGMENT_LABELS = {
    0: "Casual",       # Low engagement, moderate ratings
    1: "Enthusiast",   # High engagement, high ratings  
    2: "Critic",       # Moderate engagement, low ratings
    3: "Hardcore",     # Very high engagement, varied ratings
    4: "Explorer",     # Diverse item interactions
}

SEED = 42


def create_spark_session():
    """Create a local Spark session for ML training."""
    return (
        SparkSession.builder
        .appName("GameRec-ALS-Training")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def load_data(spark):
    """Load the cleaned Parquet data."""
    print(f"\n📂 Loading cleaned data from: {CLEANED_PARQUET_PATH}")
    df = spark.read.parquet(CLEANED_PARQUET_PATH)
    print(f"   Records loaded: {df.count():,}")
    return df


def train_initial_als(train_df, test_df):
    """
    Train an ALS model with initial hyperparameters.
    
    Returns the trained model and its RMSE on the test set.
    """
    print("\n🤖 Training initial ALS model...")
    print("   Parameters: rank=10, maxIter=10, regParam=0.1")
    
    als = ALS(
        userCol="user_idx",
        itemCol="item_idx",
        ratingCol="rating",
        coldStartStrategy="drop",  # Drop NaN predictions for cold-start users/items
        rank=10,
        maxIter=10,
        regParam=0.1,
        seed=SEED,
    )
    
    model = als.fit(train_df)
    
    # Evaluate on test set
    predictions = model.transform(test_df)
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )
    rmse = evaluator.evaluate(predictions)
    
    print(f"   Initial RMSE: {rmse:.4f}")
    return model, rmse


def run_grid_search(train_df, test_df):
    """
    Run a hyperparameter grid search using CrossValidator when initial RMSE > 1.5.
    
    Grid:
      rank:     [10, 20, 50]
      regParam: [0.01, 0.1, 0.5]
      maxIter:  [10, 20]
    
    Uses 3-fold cross-validation to select the best model.
    """
    print("\n🔍 RMSE > 1.5 — Running hyperparameter grid search...")
    print("   Grid: rank=[10,20,50], regParam=[0.01,0.1,0.5], maxIter=[10,20]")
    print("   Cross-validation: 3 folds")
    
    als = ALS(
        userCol="user_idx",
        itemCol="item_idx",
        ratingCol="rating",
        coldStartStrategy="drop",
        seed=SEED,
    )
    
    param_grid = (
        ParamGridBuilder()
        .addGrid(als.rank, [10, 20, 50])
        .addGrid(als.regParam, [0.01, 0.1, 0.5])
        .addGrid(als.maxIter, [10, 20])
        .build()
    )
    
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )
    
    cv = CrossValidator(
        estimator=als,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=3,
        seed=SEED,
    )
    
    cv_model = cv.fit(train_df)
    best_model = cv_model.bestModel
    
    # Evaluate best model on test set
    predictions = best_model.transform(test_df)
    rmse = evaluator.evaluate(predictions)
    
    # Extract best parameters
    best_rank = best_model.rank
    best_reg = best_model._java_obj.parent().getRegParam()
    best_iter = best_model._java_obj.parent().getMaxIter()
    
    print(f"\n   ✅ Best model found:")
    print(f"      rank={best_rank}, regParam={best_reg}, maxIter={best_iter}")
    print(f"      Test RMSE: {rmse:.4f}")
    
    return best_model, rmse


def perform_user_segmentation(spark, model, df):
    """
    Cluster users into segments using KMeans on ALS latent factors.
    
    Steps:
    1. Extract user latent factor vectors from the trained ALS model
    2. Convert the factor array column to a dense vector
    3. Apply KMeans (k=5) to group users into segments
    4. Assign descriptive labels to each cluster
    5. Save the user→segment mapping
    """
    print("\n👥 Performing user segmentation with KMeans (k=5)...")
    
    # Step 1: Get user factors — schema is (id: int, features: array<float>)
    user_factors = model.userFactors
    
    # Step 2: Convert array<float> → DenseVector for KMeans
    # We use a UDF to convert the features array to a Spark ML Vector
    array_to_vector_udf = F.udf(lambda arr: Vectors.dense(arr), VectorUDT())
    user_factors = user_factors.withColumn(
        "features_vec", array_to_vector_udf(F.col("features"))
    )
    
    # Step 3: Apply KMeans clustering
    kmeans = KMeans(
        featuresCol="features_vec",
        predictionCol="segment",
        k=5,
        seed=SEED,
    )
    kmeans_model = kmeans.fit(user_factors)
    segmented = kmeans_model.transform(user_factors)
    
    # Step 4: Create user→segment mapping with descriptive labels
    # Map cluster IDs to descriptive names
    segment_label_map = F.create_map(
        *[item for k, v in SEGMENT_LABELS.items() for item in (F.lit(k), F.lit(v))]
    )
    
    user_segments = (
        segmented
        .select(
            F.col("id").alias("user_idx"),
            F.col("segment"),
            segment_label_map[F.col("segment")].alias("segment_label")
        )
    )
    
    # Print segment distribution
    print("\n   Segment Distribution:")
    print("   " + "-" * 50)
    seg_dist = user_segments.groupBy("segment", "segment_label").count().orderBy("segment").collect()
    total_users = user_segments.count()
    for row in seg_dist:
        pct = row["count"] / total_users * 100
        bar = "█" * int(pct / 2)
        print(f"   [{row['segment']}] {row['segment_label']:<12}  →  {row['count']:>6,}  ({pct:5.1f}%)  {bar}")
    
    # Step 5: Save user segments
    user_segments.write.mode("overwrite").parquet(USER_SEGMENTS_PATH)
    print(f"\n   User segments saved to: {USER_SEGMENTS_PATH}")
    
    return user_segments


def precompute_segment_top5(spark, df, user_segments):
    """
    Precompute the Top-5 highest avg-rated items for each user segment.
    
    This is used as a fallback for cold-start users: we assign them to the
    nearest segment and serve these precomputed recommendations.
    """
    print("\n🏆 Precomputing segment Top-5 items...")
    
    # Join ratings with segment info
    df_with_segments = df.join(user_segments, on="user_idx", how="inner")
    
    # For each segment, compute avg rating per item, then pick top 5
    from pyspark.sql.window import Window
    
    segment_item_ratings = (
        df_with_segments
        .groupBy("segment", "segment_label", "item_idx")
        .agg(
            F.avg("rating").alias("avg_rating"),
            F.count("*").alias("num_ratings")
        )
        # Require at least 3 ratings to avoid noise from single-review items
        .filter(F.col("num_ratings") >= 3)
    )
    
    # Rank items within each segment by average rating
    window_spec = Window.partitionBy("segment").orderBy(F.col("avg_rating").desc())
    segment_top5 = (
        segment_item_ratings
        .withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") <= 5)
    )
    
    # Display results
    print("\n   Segment Top-5 Items:")
    for seg_id in range(5):
        label = SEGMENT_LABELS.get(seg_id, f"Segment-{seg_id}")
        print(f"\n   [{seg_id}] {label}:")
        items = segment_top5.filter(F.col("segment") == seg_id).orderBy("rank").collect()
        for item in items:
            print(f"       #{item['rank']}  item_idx={item['item_idx']}  "
                  f"avg_rating={item['avg_rating']:.2f}  ({item['num_ratings']} reviews)")
    
    # Save
    segment_top5.write.mode("overwrite").parquet(SEGMENT_TOP5_PATH)
    print(f"\n   Segment Top-5 saved to: {SEGMENT_TOP5_PATH}")
    
    return segment_top5


def main():
    """Main entry point — ALS training + user segmentation pipeline."""
    print("=" * 60)
    print("🎮 GAME RECOMMENDATION SYSTEM — Phase 2: ALS Training")
    print("=" * 60)
    
    spark = create_spark_session()
    
    try:
        # Load data
        df = load_data(spark)
        
        # Split 80/20
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=SEED)
        print(f"\n   Train set: {train_df.count():,} records")
        print(f"   Test set:  {test_df.count():,} records")
        
        # Train initial model
        model, rmse = train_initial_als(train_df, test_df)
        
        # If RMSE > 1.5, run grid search for better hyperparameters
        if rmse > 1.5:
            model, rmse = run_grid_search(train_df, test_df)
        else:
            print(f"\n   ✅ RMSE {rmse:.4f} ≤ 1.5 — No grid search needed")
        
        # Save the best model
        model.write().overwrite().save(ALS_MODEL_PATH)
        print(f"\n   💾 ALS model saved to: {ALS_MODEL_PATH}")
        
        # ─── User Segmentation ──────────────────────────────────────
        user_segments = perform_user_segmentation(spark, model, df)
        
        # Precompute segment Top-5 for cold-start fallback
        precompute_segment_top5(spark, df, user_segments)
        
        # ─── Final Summary ──────────────────────────────────────────
        print("\n" + "=" * 60)
        print("📊 TRAINING SUMMARY")
        print("=" * 60)
        print(f"   Final RMSE:         {rmse:.4f}")
        print(f"   Model rank:         {model.rank}")
        print(f"   User factors shape: ({model.userFactors.count()}, {model.rank})")
        print(f"   Item factors shape: ({model.itemFactors.count()}, {model.rank})")
        print(f"   Segments:           5 (KMeans)")
        print("=" * 60)
        
        print("\n✅ Phase 2 complete! Ready for Phase 3 (Kafka Producer).")
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
