"""
Phase 3 — Kafka Producer

Replay-based simulation that reads from the cleaned Parquet dataset and
streams events to Kafka topic "game-events" with 2 partitions.

Design Decisions:
- Uses replay from cleaned dataset (NOT external APIs) to avoid rate limits
  and ensure ID-space consistency with the trained ALS model.
- Partitions by (user_idx % 2) so each partition handles a distinct user 
  population, enabling parallel consumption with user-locality guarantees.
- Injects cold-start users (15%) and malformed records (5%) for testing
  downstream error handling and fallback recommendation logic.

Requires: Kafka broker running on localhost:9092
"""

import os
import sys
import json
import time
import random
from datetime import datetime

import pandas as pd
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

# ─── Configuration ──────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CLEANED_PARQUET_PATH = os.path.join(PROJECT_ROOT, "data", "games_cleaned.parquet")

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_NAME = "game-events"
NUM_PARTITIONS = 2
REPLICATION_FACTOR = 1

# Injection probabilities for testing downstream resilience
COLD_START_PROBABILITY = 0.15    # 15% chance of cold-start user (user_idx = -1)
MALFORMED_PROBABILITY = 0.05     # 5% chance of malformed record

# Delay between messages (seconds)
MIN_DELAY = 0.3
MAX_DELAY = 1.5


def create_topic():
    """
    Create the Kafka topic if it doesn't already exist.
    
    We explicitly set 2 partitions to enable our user-based partitioning
    strategy: even user_idx → partition 0, odd user_idx → partition 1.
    """
    print(f"\n📡 Creating Kafka topic '{TOPIC_NAME}' with {NUM_PARTITIONS} partitions...")
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
        topic = NewTopic(
            name=TOPIC_NAME,
            num_partitions=NUM_PARTITIONS,
            replication_factor=REPLICATION_FACTOR,
        )
        admin.create_topics([topic])
        print(f"   ✅ Topic '{TOPIC_NAME}' created successfully")
    except TopicAlreadyExistsError:
        print(f"   ℹ️  Topic '{TOPIC_NAME}' already exists — reusing")
    except NoBrokersAvailable:
        print(f"   ❌ No Kafka brokers available at {KAFKA_BOOTSTRAP}")
        print("   Please start Kafka first.")
        sys.exit(1)
    except Exception as e:
        print(f"   ⚠️  Topic creation warning: {e}")


def load_dataset():
    """
    Load the cleaned Parquet dataset into a Pandas DataFrame for sampling.
    
    We use Pandas here (instead of PySpark) because the producer just needs
    fast random sampling — no distributed computation required.
    """
    print(f"\n📂 Loading dataset from: {CLEANED_PARQUET_PATH}")
    
    if not os.path.exists(CLEANED_PARQUET_PATH):
        print(f"   ❌ File not found: {CLEANED_PARQUET_PATH}")
        print("   Please run preprocessing.py first (Phase 1)")
        sys.exit(1)
    
    df = pd.read_parquet(CLEANED_PARQUET_PATH)
    print(f"   Loaded {len(df):,} records for replay simulation")
    return df


def build_payload(row, inject_cold_start=False, inject_malformed=False):
    """
    Build a JSON payload from a dataset row, with optional fault injection.
    
    Normal payload:
    {
        "user_id": <int user_idx>,
        "item_id": <int item_idx>,
        "rating": <float>,
        "timestamp": "<ISO8601 now>"
    }
    
    Cold-start: user_id is set to -1 (unknown user)
    Malformed:  randomly drop fields or corrupt types
    """
    if inject_malformed:
        # Inject various types of malformation for testing dead-letter routing
        malform_type = random.choice(["missing_field", "wrong_type", "empty_json", "bad_rating"])
        if malform_type == "missing_field":
            # Drop a required field
            return {"item_id": int(row["item_idx"]), "timestamp": datetime.now().isoformat()}
        elif malform_type == "wrong_type":
            # Send string where int expected
            return {
                "user_id": "not_a_number",
                "item_id": "also_bad",
                "rating": "five_stars",
                "timestamp": datetime.now().isoformat()
            }
        elif malform_type == "empty_json":
            return {}
        elif malform_type == "bad_rating":
            # Rating outside valid range
            return {
                "user_id": int(row["user_idx"]),
                "item_id": int(row["item_idx"]),
                "rating": 99.9,
                "timestamp": datetime.now().isoformat()
            }
    
    user_id = -1 if inject_cold_start else int(row["user_idx"])
    
    return {
        "user_id": user_id,
        "item_id": int(row["item_idx"]),
        "rating": float(row["rating"]),
        "timestamp": datetime.now().isoformat()
    }


def determine_partition(user_id):
    """
    Partition assignment strategy: user_idx % 2
    
    This ensures that all events for a given user land on the same partition,
    providing user-level ordering guarantees. Even-indexed users go to
    partition 0, odd-indexed to partition 1.
    
    Cold-start users (user_id = -1) always go to partition 1 (since -1 % 2 = 1).
    """
    if user_id < 0:
        return 1  # Cold-start users → partition 1
    return user_id % NUM_PARTITIONS


def run_producer(df):
    """
    Main producer loop — indefinitely samples and sends events to Kafka.
    
    For each event:
    1. Sample a random row from the cleaned dataset
    2. Roll dice for cold-start injection (15%) or malformed injection (5%)
    3. Build the JSON payload
    4. Send to the appropriate partition based on user_idx % 2
    5. Sleep for a random delay between 0.3s and 1.5s
    """
    print("\n🚀 Starting Kafka producer...")
    print(f"   Bootstrap: {KAFKA_BOOTSTRAP}")
    print(f"   Topic:     {TOPIC_NAME}")
    print(f"   Partitions: {NUM_PARTITIONS}")
    print(f"   Cold-start injection rate: {COLD_START_PROBABILITY * 100:.0f}%")
    print(f"   Malformed injection rate:  {MALFORMED_PROBABILITY * 100:.0f}%")
    print(f"   Delay range: {MIN_DELAY}s – {MAX_DELAY}s")
    print("-" * 60)
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
    )
    
    event_count = 0
    cold_start_count = 0
    malformed_count = 0
    
    try:
        while True:
            # Sample a random row
            row = df.sample(n=1).iloc[0]
            
            # Determine injection type (mutually exclusive: malformed takes priority)
            roll = random.random()
            inject_malformed = roll < MALFORMED_PROBABILITY
            inject_cold_start = (not inject_malformed) and (roll < MALFORMED_PROBABILITY + COLD_START_PROBABILITY)
            
            # Build payload
            payload = build_payload(row, inject_cold_start, inject_malformed)
            
            # Determine partition (use -1 for malformed to keep them on partition 1)
            user_id = payload.get("user_id", -1)
            if not isinstance(user_id, int):
                user_id = -1  # Malformed payloads go to partition 1
            partition = determine_partition(user_id)
            
            # Send to Kafka
            producer.send(
                topic=TOPIC_NAME,
                key=str(user_id),
                value=payload,
                partition=partition,
            )
            
            event_count += 1
            if inject_cold_start:
                cold_start_count += 1
            if inject_malformed:
                malformed_count += 1
            
            # Log event
            event_type = "🆕 COLD-START" if inject_cold_start else ("💀 MALFORMED" if inject_malformed else "✅ NORMAL")
            print(f"   [{event_count:>5}] {event_type} | "
                  f"user={str(user_id):>6} | "
                  f"partition={partition} | "
                  f"payload_keys={list(payload.keys())}")
            
            # Periodic summary every 50 events
            if event_count % 50 == 0:
                print(f"\n   📊 Summary: {event_count} total | "
                      f"{cold_start_count} cold-start ({cold_start_count/event_count*100:.1f}%) | "
                      f"{malformed_count} malformed ({malformed_count/event_count*100:.1f}%)\n")
            
            # Random delay to simulate realistic event spacing
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
    
    except KeyboardInterrupt:
        print(f"\n\n🛑 Producer stopped after {event_count} events")
        print(f"   Cold-start: {cold_start_count} ({cold_start_count/max(event_count,1)*100:.1f}%)")
        print(f"   Malformed:  {malformed_count} ({malformed_count/max(event_count,1)*100:.1f}%)")
    finally:
        producer.flush()
        producer.close()


def main():
    """Main entry point — setup and launch the Kafka producer."""
    print("=" * 60)
    print("🎮 GAME RECOMMENDATION SYSTEM — Phase 3: Kafka Producer")
    print("=" * 60)
    
    # Create topic (idempotent)
    create_topic()
    
    # Load dataset
    df = load_dataset()
    
    # Start producing
    run_producer(df)


if __name__ == "__main__":
    main()
