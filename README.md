# 🎮 Real-Time Game Recommendation System

A complete end-to-end Big Data project implementing a Real-Time Game Recommendation System using **Apache Spark**, **Kafka**, and **Spark Structured Streaming** with personalized recommendations, cold-start handling, and user segmentation.

## 📋 Table of Contents
- [Architecture](#architecture)
- [Dataset](#dataset)
- [Project Structure](#project-structure)
- [Setup & Installation](#setup--installation)
- [Running the Pipeline](#running-the-pipeline)
- [Phase Details](#phase-details)
- [Late Data Handling Policy](#late-data-handling-policy)

## 🏗️ Architecture

```
[Amazon Dataset] → [PySpark Preprocessing] → [ALS Model Training]
                                                      ↓
[Kafka Producer] → [Kafka Topic: game-events] → [Spark Structured Streaming]
     ↑                    (2 partitions)                   ↓
[Sampled from               ↓                    [Window Analytics (30s/10s)]
 cleaned dataset]    [JSON parsing +                       ↓
                      malformed handling]         [ML + Streaming Integration]
                                                           ↓
                                              [Recommendations + Alerts + Dashboard]
```

## 📊 Dataset

**Amazon Video Games 5-core** (UCSD JMCAULEY)
- **URL**: https://datarepos.ucsd.edu/dataset/amazon/Video_Games_5.json.gz
- **Size**: ~500K+ records
- **Schema**: `reviewerID`, `asin`, `overall` (1-5), `unixReviewTime`

## 📁 Project Structure

```
MiniProject3/
├── code/
│   ├── preprocessing.py        # Phase 1: Data preprocessing
│   ├── train_als.py            # Phase 2: ALS training + segmentation
│   ├── kafka_producer.py       # Phase 3: Kafka event producer
│   ├── streaming_pipeline.py   # Phase 4: Spark Structured Streaming
│   ├── recommendation_engine.py # Phase 5: ML + streaming integration
│   └── dashboard.py            # Phase 6: Streamlit dashboard
├── models/
│   ├── als_model/              # Trained ALS model
│   ├── user_indexer/           # Fitted StringIndexer (users)
│   └── item_indexer/           # Fitted StringIndexer (items)
├── data/
│   ├── Video_Games_5.json      # Raw dataset (download required)
│   ├── games_cleaned.parquet   # Cleaned & indexed data
│   ├── user_segments.parquet   # User → segment mapping
│   └── segment_top5.parquet    # Precomputed Top-5 per segment
└── README.md
```

## ⚙️ Setup & Installation

### Prerequisites
- Python 3.10+
- Java 8/11 (for Spark)
- Apache Kafka (running on localhost:9092)

### Install Dependencies

```bash
pip install pyspark==3.5.0 kafka-python==2.0.2 pandas pyarrow streamlit plotly
```

### Download Dataset

```bash
# Download and extract the dataset
wget https://datarepos.ucsd.edu/dataset/amazon/Video_Games_5.json.gz -O data/Video_Games_5.json.gz
gzip -d data/Video_Games_5.json.gz
```

### Start Kafka

```bash
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka broker
bin/kafka-server-start.sh config/server.properties
```

## 🚀 Running the Pipeline

Execute each phase sequentially:

```bash
# Phase 1: Preprocess the dataset
python code/preprocessing.py

# Phase 2: Train ALS model + user segmentation
python code/train_als.py

# Phase 3: Start Kafka producer (runs indefinitely)
python code/kafka_producer.py

# Phase 4: Start streaming pipeline (in a new terminal)
python code/streaming_pipeline.py

# Phase 5: Start recommendation engine (in a new terminal)
python code/recommendation_engine.py

# Phase 6: Launch dashboard (in a new terminal)
streamlit run code/dashboard.py
```

## 📝 Phase Details

### Phase 1 — Data Preprocessing
- Loads raw JSON, selects/renames columns
- Drops nulls, filters invalid ratings
- Deduplicates (user, item) pairs keeping most recent
- Applies StringIndexer for integer indices
- Saves Parquet + indexer models

### Phase 2 — ALS Training
- 80/20 train/test split (seed=42)
- Initial ALS: rank=10, maxIter=10, regParam=0.1
- Auto grid search if RMSE > 1.5
- KMeans (k=5) user segmentation on latent factors
- Precomputes segment Top-5 for cold-start fallback

### Phase 3 — Kafka Producer
- Replay-based simulation from cleaned data
- 2 partitions (user_idx % 2)
- 15% cold-start injection (user_id = -1)
- 5% malformed record injection

### Phase 4 — Structured Streaming
- Reads from Kafka with safe JSON parsing
- Dead-letter routing for malformed records
- 30s window / 10s slide with 15s watermark
- Engagement score: (count × avg_rating) / 30
- Trending alerts (avg > 4.5, count > 3)
- Activity spike alerts (count > 10)

### Phase 5 — Recommendation Engine
- Known users: ALS Top-5 + segment label
- Cold-start: majority vote → segment fallback
- Latency tracking (< 5s target)

### Phase 6 — Dashboard
- Auto-refreshes every 3 seconds
- 5 panels: Metrics, Recommendations, Trending, Activity, Alerts
- Premium glassmorphism dark theme

## 📐 Late Data Handling Policy

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Watermark | 15 seconds | Data arriving >15s late is dropped |
| Window | 30 seconds | Aggregation window duration |
| Slide | 10 seconds | Window slide interval |
| Policy | DROP | Late data (>15s) cannot fill a complete slide interval and would corrupt window boundaries. Dropping is preferable to processing stale data that may no longer reflect real user intent. |

## 🛠️ Environment

- Python 3.10+
- PySpark 3.5
- kafka-python 2.0
- Streamlit 1.35
- Kafka broker: localhost:9092
- Spark master: local[*]
