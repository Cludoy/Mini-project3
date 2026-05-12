"""
Phase 4 — Kafka Producer

Replay-based simulation from cleaned dataset. Sends events to "game-events"
topic with 2 partitions (partitioned by user_idx % 2).

Injects:
  - 15% cold-start users (user_id = -1)
  - 5% malformed records (corrupted fields)

Partitioning justification: user_idx % 2 ensures all events for the same user
land on the same partition, preserving per-user ordering and enabling stateful
aggregations without cross-partition shuffles.
"""

import os
import sys
import json
import time
import random
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CLEANED_PATH = os.path.join(PROJECT_ROOT, "data", "games_cleaned.parquet")

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "game-events"


def setup_topic():
    """Create topic with 2 partitions if it doesn't exist."""
    print(f"\n[Ensuring topic '{TOPIC}' exists (2 partitions)...]")
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
        admin.create_topics([NewTopic(name=TOPIC, num_partitions=2, replication_factor=1)])
        print("   [OK] Topic created")
    except TopicAlreadyExistsError:
        print("   [INFO] Topic already exists")
    except NoBrokersAvailable:
        print(f"   [ERROR] No brokers at {KAFKA_BOOTSTRAP}. Start Kafka first.")
        sys.exit(1)
    except Exception as e:
        print(f"   [WARN] {e}")


def main():
    print("=" * 55)
    print("[PROJECT NEXUS — Phase 4: Kafka Producer]")
    print("=" * 55)

    setup_topic()

    if not os.path.exists(CLEANED_PATH):
        print(f"[ERROR] {CLEANED_PATH} not found. Run preprocessing.py first.")
        sys.exit(1)

    df = pd.read_parquet(CLEANED_PATH)[["user_idx", "item_idx", "rating"]]
    print(f"\n[Loaded] {len(df):,} records for replay")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    sent = cold = bad = 0
    print(f"\n[Producing to '{TOPIC}' — Ctrl+C to stop]")
    print(f"   Cold-start: 15%  |  Malformed: 5%  |  Delay: 0.3–1.5s")
    print("-" * 55)

    try:
        while True:
            roll = random.random()

            if roll < 0.05:
                # Malformed record
                event = {"user_id": "CORRUPTED", "item_id": None,
                         "rating": "N/A", "timestamp": "bad-ts"}
                partition = random.randint(0, 1)
                bad += 1
                tag = "[MALFORMED]"

            elif roll < 0.20:
                # Cold-start user
                event = {
                    "user_id": -1,
                    "item_id": random.randint(0, 5000),
                    "rating": round(random.uniform(1, 5), 1),
                    "timestamp": datetime.now().isoformat(),
                }
                partition = 0
                cold += 1
                tag = "[COLD-START]"

            else:
                # Normal replay
                row = df.sample(1).iloc[0]
                event = {
                    "user_id": int(row["user_idx"]),
                    "item_id": int(row["item_idx"]),
                    "rating": float(row["rating"]),
                    "timestamp": datetime.now().isoformat(),
                }
                partition = int(row["user_idx"]) % 2
                tag = "[NORMAL]"

            producer.send(TOPIC, value=event, partition=partition)
            sent += 1
            print(f"   [{sent:>5}] {tag} | p={partition} | {event}")

            if sent % 50 == 0:
                print(f"\n   [STATS] Total={sent} | Cold={cold} ({cold/sent*100:.0f}%) "
                      f"| Bad={bad} ({bad/sent*100:.0f}%)\n")

            time.sleep(random.uniform(0.3, 1.5))

    except KeyboardInterrupt:
        print(f"\n[Stopped after {sent} events (cold={cold}, bad={bad})]")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
