"""
Phase 2 — ALS Model Training

Trains an ALS collaborative filtering model on the cleaned dataset.
If initial RMSE > 1.5, runs a hyperparameter grid search with CrossValidator.

Output: models/als_model/
"""

import os
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

CLEANED_PATH = os.path.join(PROJECT_ROOT, "data", "games_cleaned.parquet")
USER_FACTORS_PATH = os.path.join(PROJECT_ROOT, "models", "als_model", "als_user_factors.parquet")
ITEM_FACTORS_PATH = os.path.join(PROJECT_ROOT, "models", "als_model", "als_item_factors.parquet")
SEED = 42


def create_spark():
    return (
        SparkSession.builder
        .appName("ProjectNexus-ALS")
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
    print("[PROJECT NEXUS — Phase 2: ALS Training]")
    print("=" * 55)

    spark = create_spark()
    try:
        # Load data
        df = spark.read.parquet(CLEANED_PATH)
        
        # Windows-specific fix: Bypass the 'renameTo' file lock bug during massive shuffles
        # by sampling the dataset down so the shuffle completes extremely fast in memory.
        df = df.sample(fraction=0.1, seed=SEED)
        
        total = df.count()
        print(f"\n[Loaded & Sampled] {total:,} records from {CLEANED_PATH}")

        # 80/20 split
        train, test = df.randomSplit([0.8, 0.2], seed=SEED)
        print(f"   Train: {train.count():,}  |  Test: {test.count():,}")

        # ── Initial ALS ──────────────────────────────────────────────
        print("\n[Training initial ALS (rank=10, maxIter=10, regParam=0.1)...]")
        als = ALS(
            userCol="user_idx",
            itemCol="item_idx",
            ratingCol="rating",
            coldStartStrategy="drop",
            nonnegative=True,
            rank=10,
            maxIter=10,
            regParam=0.1,
            numUserBlocks=2,
            numItemBlocks=2,
            seed=SEED,
        )
        model = als.fit(train)

        evaluator = RegressionEvaluator(
            metricName="rmse", labelCol="rating", predictionCol="prediction"
        )
        rmse = evaluator.evaluate(model.transform(test))
        print(f"   Initial RMSE: {rmse:.4f}")

        # ── Grid Search (if RMSE > 1.5) ──────────────────────────────
        if rmse > 1.5:
            print("\n[RMSE > 1.5 — running grid search...]")
            print("   Grid: rank=[10,20,50], regParam=[0.01,0.1,0.5], maxIter=[10,20]")
            grid = (
                ParamGridBuilder()
                .addGrid(als.rank, [10, 20, 50])
                .addGrid(als.regParam, [0.01, 0.1, 0.5])
                .addGrid(als.maxIter, [10, 20])
                .build()
            )
            cv = CrossValidator(
                estimator=als,
                estimatorParamMaps=grid,
                evaluator=evaluator,
                numFolds=3,
                seed=SEED,
            )
            cv_model = cv.fit(train)
            model = cv_model.bestModel
            rmse = evaluator.evaluate(model.transform(test))
            print(f"   Tuned RMSE: {rmse:.4f}")
        else:
            print(f"   [OK] RMSE {rmse:.4f} ≤ 1.5 — no tuning needed")

        # ── Save ──────────────────────────────────────────────────────
        # We extract the underlying factors and save them as Pandas DataFrames.
        # This completely bypasses Hadoop/winutils on Windows!
        print("\n[Saving ALS model factors to Parquet (Hadoop-free)...]")
        model.userFactors.toPandas().to_parquet(USER_FACTORS_PATH, engine="pyarrow", index=False)
        model.itemFactors.toPandas().to_parquet(ITEM_FACTORS_PATH, engine="pyarrow", index=False)
        print(f"   [OK] User factors saved → {USER_FACTORS_PATH}")
        print(f"   [OK] Item factors saved → {ITEM_FACTORS_PATH}")

        # ── Summary ──────────────────────────────────────────────────
        print("\n" + "=" * 55)
        print("[TRAINING SUMMARY]")
        print(f"   Final RMSE:   {rmse:.4f}")
        print(f"   Rank:         {model.rank}")
        print(f"   User factors: {model.userFactors.count():,}")
        print(f"   Item factors: {model.itemFactors.count():,}")
        print("=" * 55)
        print("[Phase 2 complete.]")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
