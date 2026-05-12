"""
Phase 6 — ML + Streaming Integration (Hadoop-Free Architecture)

Provides a recommendation engine that serves predictions for users in real-time.
- Known users: Computes Top-5 predictions manually via dot-product of Pandas ALS factors.
- Cold-start users (e.g. user_id = -1): Gets Top-5 trending items from the live global window.
- Known users with no ALS history: Falls back to precomputed Top-5 for their segment.

Bypasses PySpark ML loads to completely avoid Hadoop/winutils errors on Windows!
"""

import os
import time
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

USER_FACTORS_PATH = os.path.join(PROJECT_ROOT, "data", "als_user_factors.parquet")
ITEM_FACTORS_PATH = os.path.join(PROJECT_ROOT, "data", "als_item_factors.parquet")
SEGMENTS_PATH = os.path.join(PROJECT_ROOT, "data", "user_segments.parquet")
TOP5_PATH = os.path.join(PROJECT_ROOT, "data", "segment_top5.parquet")


class RecommendationEngine:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        print("\n[Initializing Recommendation Engine...]")

        # ── Load ALS factors into Pandas for extremely fast inference ──
        print(f"   [Loading ALS factors via Pandas]")
        try:
            user_factors = pd.read_parquet(USER_FACTORS_PATH)
            self.user_vecs = user_factors.set_index("id")["features"].apply(np.array).to_dict()
            
            item_factors = pd.read_parquet(ITEM_FACTORS_PATH)
            self.item_ids = item_factors["id"].values
            self.item_matrix = np.vstack(item_factors["features"].values)
            self.has_als = True
        except Exception as e:
            print(f"   [ERROR] Failed to load ALS factors. Run train_als.py first.\nError: {e}")
            self.has_als = False

        # ── Load Segments & Top-5 via Pandas ──
        print(f"   [Loading User Segments & Fallbacks via Pandas]")
        try:
            self.user_segs = pd.read_parquet(SEGMENTS_PATH).set_index("user_idx")
            self.seg_top5 = pd.read_parquet(TOP5_PATH)
        except Exception as e:
            print(f"   [ERROR] Failed to load Segment data. Run train_kmeans.py first.\nError: {e}")
            self.user_segs = pd.DataFrame()
            self.seg_top5 = pd.DataFrame()

        print("   [Engine ready.]\n")

    def recommend_for_user(self, user_id: int) -> dict:
        """Generate recommendations for a given user, enforcing latency < 5s."""
        start_time = time.time()
        recs = []
        path = ""

        if user_id == -1 or not self.has_als:
            # Cold-start: serve global top-5 trending from stream window (if available)
            try:
                if self.spark:
                    trending = self.spark.sql("""
                        SELECT item_id, engagement_score
                        FROM window_metrics
                        ORDER BY engagement_score DESC
                        LIMIT 5
                    """).toPandas()
                    
                    if not trending.empty:
                        recs = [{"item_id": int(r["item_id"]), "score": float(r["engagement_score"])} for _, r in trending.iterrows()]
                        path = "cold_start_global"
                    else:
                        raise ValueError("Empty window")
            except Exception:
                # Fallback to segment 0
                if not self.seg_top5.empty:
                    rows = self.seg_top5[self.seg_top5["segment"] == 0]
                    recs = [{"item_id": int(r["item_idx"]), "score": float(r["avg_rating"])} for _, r in rows.iterrows()]
                path = "default_fallback"
        else:
            # Known user — check if they exist in ALS
            if user_id in self.user_vecs:
                try:
                    # Fast matrix multiplication for all items
                    u_vec = self.user_vecs[user_id]
                    scores = self.item_matrix.dot(u_vec)
                    
                    # Get top 5 indices
                    top_indices = np.argsort(scores)[::-1][:5]
                    
                    # Extract segment info
                    segment = "Unknown"
                    if user_id in self.user_segs.index:
                        segment = self.user_segs.loc[user_id, "segment_label"]
                        # Handle potential duplicate index return as Series
                        if isinstance(segment, pd.Series):
                            segment = segment.iloc[0]
                    
                    recs = [{"item_id": int(self.item_ids[i]), "score": float(scores[i])} for i in top_indices]
                    path = f"als_segment_{segment}"
                except Exception as e:
                    print(f"Error in ALS calculation: {e}")
                    path = "error"
            else:
                # User not in ALS -> use their segment fallback or default
                if user_id in self.user_segs.index:
                    seg_id = self.user_segs.loc[user_id, "segment"]
                    if isinstance(seg_id, pd.Series): seg_id = seg_id.iloc[0]
                    
                    rows = self.seg_top5[self.seg_top5["segment"] == seg_id]
                    recs = [{"item_id": int(r["item_idx"]), "score": float(r["avg_rating"])} for _, r in rows.iterrows()]
                    path = f"segment_fallback_{seg_id}"
                else:
                    if not self.seg_top5.empty:
                        rows = self.seg_top5[self.seg_top5["segment"] == 0]
                        recs = [{"item_id": int(r["item_idx"]), "score": float(r["avg_rating"])} for _, r in rows.iterrows()]
                    path = "default_fallback"

        latency_ms = (time.time() - start_time) * 1000
        print(f"[REC] user={user_id:4} | path={path:20} | latency={latency_ms:6.1f}ms")
        
        # SLA enforcement log
        if latency_ms >= 5000:
            print(f"      [WARN] SLA Breached: {latency_ms:.1f}ms (Target < 5000ms)")

        return {
            "user_id": user_id,
            "path": path,
            "recommendations": recs,
            "latency_ms": latency_ms
        }


def main():
    print("=" * 55)
    print("[PROJECT NEXUS — Phase 6: Recommendation Engine Tests]")
    print("=" * 55)
    
    # We don't even need a fully configured Spark session for local tests anymore!
    spark = SparkSession.builder.appName("RecEngine-Test").master("local[*]").getOrCreate()
    engine = RecommendationEngine(spark)
    
    # Test cases
    test_users = [-1, 10, 500, 9999]  # -1 is cold start, others might be known or fallback
    
    print("[Running tests:]")
    for uid in test_users:
        engine.recommend_for_user(uid)
        
    spark.stop()


if __name__ == "__main__":
    main()
