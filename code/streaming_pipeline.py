"""
Native Python Streaming Pipeline (HADOOP-FREE)

Reads from Kafka "game-events", parses JSON safely (dead-letter routing),
applies windowed analytics (30s/10s), computes engagement scores, and
generates trending/spike alerts.

This replaces Spark Structured Streaming entirely to honor the NO-HADOOP architecture.
It uses native kafka-python-ng and Pandas.
"""

import os
import json
import time
from datetime import datetime, timedelta
import pandas as pd
from kafka import KafkaConsumer

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
METRICS_PATH = os.path.join(PROJECT_ROOT, "data", "window_metrics.csv")
ALERTS_PATH = os.path.join(PROJECT_ROOT, "data", "alert_feed.csv")

def main():
    print("=" * 55)
    print("[PROJECT NEXUS — Native Streaming Pipeline]")
    print("=" * 55)

    print("\n[Connecting to Kafka Broker @ localhost:9092...]")
    consumer = KafkaConsumer(
        'game-events',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: x.decode('utf-8')
    )
    print("   [OK] Connected to 'game-events' topic.")

    event_buffer = []
    alert_buffer = []
    
    last_window_time = time.time()
    
    print("\n[Starting pure-Python streaming loop. Ctrl+C to stop.]\n")
    
    try:
        for message in consumer:
            raw_value = message.value
            try:
                data = json.loads(raw_value)
                user_id = data.get("user_id")
                item_id = data.get("item_id")
                rating = data.get("rating")
                ts_str = data.get("timestamp")
                
                if user_id is None or item_id is None or rating is None or ts_str is None:
                    # print(f"[DEAD-LETTER] Malformed JSON: {raw_value}")
                    continue
                    
                rating = float(rating)
                if rating < 1.0 or rating > 5.0:
                    print(f"[DEAD-LETTER] Invalid rating: {rating}")
                    continue
                    
                event_time = datetime.fromisoformat(ts_str)
                event_buffer.append({
                    "user_id": user_id,
                    "item_id": item_id,
                    "rating": rating,
                    "event_time": event_time
                })
                
            except json.JSONDecodeError:
                print(f"[DEAD-LETTER] Unparseable JSON: {raw_value}")
                continue

            # Process 10-second sliding windows
            current_time = time.time()
            if current_time - last_window_time >= 10.0:
                if len(event_buffer) == 0:
                    last_window_time = current_time
                    continue
                    
                df = pd.DataFrame(event_buffer)
                
                # Watermark: drop events older than 15 s relative to the latest event time.
                # Late data beyond this threshold is silently discarded (dead-lettered).
                # This matches Spark Structured Streaming watermark semantics.
                watermark_threshold = df["event_time"].max() - timedelta(seconds=15)
                df = df[df["event_time"] >= watermark_threshold]
                
                # Apply 30s window
                window_threshold = df["event_time"].max() - timedelta(seconds=30)
                window_df = df[df["event_time"] >= window_threshold]
                
                if not window_df.empty:
                    # Item Aggregation (Trending)
                    item_metrics = window_df.groupby("item_id").agg(
                        avg_rating=("rating", "mean"),
                        interaction_count=("rating", "count")
                    ).reset_index()
                    item_metrics["engagement_score"] = round((item_metrics["interaction_count"] * item_metrics["avg_rating"]) / 30.0, 4)
                    
                    # User Aggregation (Spike Detection)
                    user_metrics = window_df.groupby("user_id").agg(
                        interaction_count=("rating", "count")
                    ).reset_index()
                    
                    # Generate Alerts
                    trending = item_metrics[(item_metrics["avg_rating"] > 4.5) & (item_metrics["interaction_count"] > 3)]
                    for _, row in trending.iterrows():
                        alert_buffer.append({
                            "alert_type": "TRENDING",
                            "user_id": -1,
                            "item_id": int(row["item_id"]),
                            "message": f"Trending Item #{int(row['item_id'])} (Score: {row['engagement_score']:.2f})",
                            "timestamp": datetime.now().isoformat()
                        })
                        
                    spikes = user_metrics[user_metrics["interaction_count"] > 10]
                    for _, row in spikes.iterrows():
                        alert_buffer.append({
                            "alert_type": "ACTIVITY_SPIKE",
                            "user_id": int(row["user_id"]),
                            "item_id": -1,
                            "message": f"Sudden activity spike from User #{int(row['user_id'])}",
                            "timestamp": datetime.now().isoformat()
                        })
                    
                    # Keep alerts buffer size manageable
                    if len(alert_buffer) > 50:
                        alert_buffer = alert_buffer[-50:]
                    
                    # Save to CSV for dashboard
                    os.makedirs(os.path.dirname(METRICS_PATH), exist_ok=True)
                    window_df.to_csv(os.path.join(PROJECT_ROOT, "data", "live_events.csv"), index=False)
                    item_metrics.to_csv(METRICS_PATH, index=False)
                    pd.DataFrame(alert_buffer).to_csv(ALERTS_PATH, index=False)
                    
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Window processed: {len(window_df)} events | {len(item_metrics)} unique items | {len(trending)} trending alerts")
                
                # Cleanup old events from buffer
                event_buffer = window_df.to_dict('records')
                last_window_time = current_time

    except KeyboardInterrupt:
        print("\n[Shutting down streaming pipeline...]")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
