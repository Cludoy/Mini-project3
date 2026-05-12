from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType

SCHEMA = StructType([
    StructField("user_id",   StringType(), True),
    StructField("item_id",   StringType(), True),
    StructField("rating",    FloatType(),  True),
    StructField("timestamp", StringType(), True),
])

spark = (
    SparkSession.builder
    .appName("ProjectNexus-Streaming")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "game-events")
    .option("startingOffsets", "latest")
    .load()
)

# Parse JSON — malformed rows produce nulls, filtered below (dead-letter)
parsed = (
    raw.select(
        F.from_json(F.col("value").cast("string"), SCHEMA).alias("d"),
        F.col("timestamp").alias("kafka_ts"),
    )
    .select("d.*", "kafka_ts")
)

# Dead-letter: drop rows where required fields are null or rating out of range
clean = (
    parsed
    .filter(F.col("user_id").isNotNull() & F.col("item_id").isNotNull()
            & F.col("rating").isNotNull())
    .filter((F.col("rating") >= 1.0) & (F.col("rating") <= 5.0))
    .withColumn("event_time", F.to_timestamp("timestamp"))
)

# 30-second window, 10-second slide, 15-second watermark for late data
windowed = (
    clean
    .withWatermark("event_time", "15 seconds")
    .groupBy(
        F.window("event_time", "30 seconds", "10 seconds"),
        F.col("item_id"),
    )
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.count("*").alias("interaction_count"),
    )
    .withColumn(
        "engagement_score",
        F.round((F.col("interaction_count") * F.col("avg_rating")) / 30.0, 4),
    )
)

# Alert stream: trending items (avg_rating > 4.5, count > 3)
alerts = windowed.filter(
    (F.col("avg_rating") > 4.5) & (F.col("interaction_count") > 3)
)

# Write windowed metrics to CSV for dashboard
(
    windowed.writeStream
    .outputMode("append")
    .format("csv")
    .option("path",             "data/spark_window_metrics")
    .option("checkpointLocation","data/checkpoints/window_metrics")
    .trigger(processingTime="10 seconds")
    .start()
)

# Write alerts to CSV for dashboard
(
    alerts.writeStream
    .outputMode("append")
    .format("csv")
    .option("path",             "data/spark_alerts")
    .option("checkpointLocation","data/checkpoints/alerts")
    .trigger(processingTime="10 seconds")
    .start()
)

spark.streams.awaitAnyTermination()
