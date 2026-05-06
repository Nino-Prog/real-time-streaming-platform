"""
spark_jobs/stream_processor.py
─────────────────────────────────────────────────────────────
Reads crypto price events from Kafka using Spark Structured
Streaming, computes rolling 1-minute OHLC aggregations,
detects price anomalies against a 1-hour baseline, and
writes results back to PostgreSQL.

Run via spark-submit (handled automatically by docker-compose):
  spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
               org.postgresql:postgresql:42.7.1 \
    /opt/spark-jobs/stream_processor.py
─────────────────────────────────────────────────────────────
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
)

# ── Config (env vars allow override in docker-compose) ────
KAFKA_BROKER  = os.getenv("KAFKA_BROKER",  "kafka:29092")
TOPIC         = os.getenv("KAFKA_TOPIC",   "crypto-prices")
CHECKPOINT    = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints/crypto")

JDBC_URL      = os.getenv(
    "JDBC_URL",
    "jdbc:postgresql://postgres:5432/streamdb"
)
JDBC_PROPS    = {
    "user":     os.getenv("DB_USER",     "streamuser"),
    "password": os.getenv("DB_PASSWORD", "streampass"),
    "driver":   "org.postgresql.Driver",
}

# Price deviation threshold to flag as anomaly (10%)
ANOMALY_THRESHOLD_PCT = float(os.getenv("ANOMALY_THRESHOLD_PCT", "10.0"))


# ── Spark Session ─────────────────────────────────────────
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("CryptoStreamProcessor")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )


# ── Schema for incoming Kafka JSON ────────────────────────
PRICE_SCHEMA = StructType([
    StructField("coin",            StringType(), True),
    StructField("price_usd",       DoubleType(), True),
    StructField("market_cap_usd",  DoubleType(), True),
    StructField("volume_24h_usd",  DoubleType(), True),
    StructField("change_24h_pct",  DoubleType(), True),
    StructField("timestamp",       StringType(), True),
])


# ── Write to Postgres ─────────────────────────────────────
def write_to_postgres(batch_df, batch_id: int, table: str):
    """Micro-batch writer — called by foreachBatch."""
    if batch_df.isEmpty():
        return
    count = batch_df.count()
    (
        batch_df.write
        .jdbc(url=JDBC_URL, table=table, mode="append", properties=JDBC_PROPS)
    )
    print(f"[Batch {batch_id}] Wrote {count} rows → {table}")


# ── Anomaly detection ─────────────────────────────────────
def detect_and_write_anomalies(batch_df, batch_id: int):
    """
    For each micro-batch:
      1. Read the 1-hour per-coin average from Postgres.
      2. Join with the current batch.
      3. Flag rows where price deviates > ANOMALY_THRESHOLD_PCT.
      4. Write flagged rows to crypto_anomalies.
    """
    if batch_df.isEmpty():
        return

    spark = batch_df.sparkSession

    # Pull 1-hour rolling averages directly from Postgres
    avg_df = spark.read.jdbc(
        url=JDBC_URL,
        table=(
            "(SELECT coin, AVG(price_usd) AS avg_price_1h "
            " FROM crypto_prices "
            " WHERE event_timestamp >= NOW() - INTERVAL '1 hour' "
            " GROUP BY coin) AS t"
        ),
        properties=JDBC_PROPS,
    )

    anomalies = (
        batch_df
        .join(avg_df, on="coin", how="left")
        # Fall back to current price if no history yet (first run)
        .withColumn(
            "avg_price_1h",
            F.coalesce(F.col("avg_price_1h"), F.col("price_usd"))
        )
        .withColumn(
            "deviation_pct",
            F.abs(
                (F.col("price_usd") - F.col("avg_price_1h"))
                / F.col("avg_price_1h") * 100
            )
        )
        .filter(F.col("deviation_pct") > ANOMALY_THRESHOLD_PCT)
        .select(
            "coin",
            "price_usd",
            "avg_price_1h",
            "deviation_pct",
            F.col("event_timestamp"),
        )
    )

    if not anomalies.isEmpty():
        count = anomalies.count()
        anomalies.write.jdbc(
            url=JDBC_URL,
            table="crypto_anomalies",
            mode="append",
            properties=JDBC_PROPS,
        )
        print(f"[Batch {batch_id}] Flagged {count} anomaly/anomalies")
    else:
        print(f"[Batch {batch_id}] No anomalies detected")


# ── Main ──────────────────────────────────────────────────
def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[Spark] Connecting to Kafka at {KAFKA_BROKER}, topic={TOPIC}")

    # ── Read raw stream from Kafka ─────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse JSON payload ─────────────────────────────────
    parsed = (
        raw_stream
        .select(
            F.from_json(F.col("value").cast("string"), PRICE_SCHEMA).alias("d")
        )
        .select("d.*")
        .withColumn("event_timestamp", F.to_timestamp("timestamp"))
        .filter(F.col("price_usd").isNotNull())
        .filter(F.col("price_usd") > 0)
        .drop("timestamp")
    )

    # ── Stream 1: Write raw prices to crypto_prices ────────
    prices_query = (
        parsed
        .writeStream
        .foreachBatch(
            lambda df, bid: write_to_postgres(df, bid, "crypto_prices")
        )
        .option("checkpointLocation", f"{CHECKPOINT}/prices")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # ── Stream 2: 1-minute windowed OHLC aggregations ──────
    agg_df = (
        parsed
        .withWatermark("event_timestamp", "1 minute")
        .groupBy("coin", F.window("event_timestamp", "1 minute"))
        .agg(
            F.first("price_usd").alias("open"),
            F.max("price_usd").alias("high"),
            F.min("price_usd").alias("low"),
            F.last("price_usd").alias("close"),
            F.avg("price_usd").alias("avg_price"),
            F.count("*").alias("event_count"),
        )
        .select(
            "coin",
            F.col("window.start").alias("bucket"),
            "open", "high", "low", "close", "avg_price", "event_count",
        )
    )

    agg_query = (
        agg_df
        .writeStream
        .foreachBatch(
            lambda df, bid: write_to_postgres(df, bid, "crypto_ohlc_1min")
        )
        .option("checkpointLocation", f"{CHECKPOINT}/agg")
        .trigger(processingTime="60 seconds")
        .outputMode("update")
        .start()
    )

    # ── Stream 3: Anomaly detection ────────────────────────
    anomaly_query = (
        parsed
        .writeStream
        .foreachBatch(detect_and_write_anomalies)
        .option("checkpointLocation", f"{CHECKPOINT}/anomalies")
        .trigger(processingTime="30 seconds")
        .start()
    )

    print("[Spark] All 3 streams running: prices / OHLC / anomalies")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
