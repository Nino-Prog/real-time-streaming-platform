"""
spark_jobs/stream_processor.py
─────────────────────────────────────────────────────────────
Reads crypto price events from Kafka using Spark Structured
Streaming, computes rolling aggregations, flags price anomalies,
and writes results back to PostgreSQL.

Run via spark-submit:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
               org.postgresql:postgresql:42.7.1 \
    spark_jobs/stream_processor.py
─────────────────────────────────────────────────────────────
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType
)

# ── Config ────────────────────────────────────────────────
KAFKA_BROKER  = "kafka:29092"
TOPIC         = "crypto-prices"
CHECKPOINT    = "/tmp/spark-checkpoints/crypto"

JDBC_URL      = "jdbc:postgresql://postgres:5432/streamdb"
JDBC_PROPS    = {
    "user":     "streamuser",
    "password": "streampass",
    "driver":   "org.postgresql.Driver",
}

# Price deviation threshold to flag as anomaly (10%)
ANOMALY_THRESHOLD_PCT = 10.0


# ── Spark Session ─────────────────────────────────────────
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("CryptoStreamProcessor")
        .master("spark://spark-master:7077")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ── Schema for incoming Kafka JSON ────────────────────────
PRICE_SCHEMA = StructType([
    StructField("coin",            StringType(),    True),
    StructField("price_usd",       DoubleType(),    True),
    StructField("market_cap_usd",  DoubleType(),    True),
    StructField("volume_24h_usd",  DoubleType(),    True),
    StructField("change_24h_pct",  DoubleType(),    True),
    StructField("timestamp",       StringType(),    True),
])


# ── Write to Postgres ─────────────────────────────────────
def write_to_postgres(batch_df, batch_id: int, table: str):
    """Micro-batch writer — called by foreachBatch."""
    if batch_df.isEmpty():
        return
    (
        batch_df.write
        .jdbc(url=JDBC_URL, table=table, mode="append", properties=JDBC_PROPS)
    )
    print(f"[Batch {batch_id}] Wrote {batch_df.count()} rows to {table}")


# ── Anomaly detection ─────────────────────────────────────
def detect_anomalies(df):
    """
    Flag rows where price deviates more than ANOMALY_THRESHOLD_PCT
    from the 1-hour rolling average for that coin.
    """
    window_1h = (
        F.window("event_timestamp", "1 hour")
    )
    avg_df = (
        df.groupBy("coin", window_1h)
        .agg(F.avg("price_usd").alias("avg_price_1h"))
        .select("coin", "window.start", "avg_price_1h")
        .withColumnRenamed("start", "window_start")
    )

    joined = df.join(avg_df, on="coin", how="left")

    anomalies = joined.withColumn(
        "deviation_pct",
        F.abs((F.col("price_usd") - F.col("avg_price_1h")) / F.col("avg_price_1h") * 100)
    ).filter(F.col("deviation_pct") > ANOMALY_THRESHOLD_PCT)

    return anomalies.select(
        "coin", "price_usd", "avg_price_1h", "deviation_pct", "event_timestamp"
    )


# ── Main ──────────────────────────────────────────────────
def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # Parse JSON payload
    parsed = (
        raw_stream
        .select(F.from_json(
            F.col("value").cast("string"), PRICE_SCHEMA
        ).alias("data"))
        .select("data.*")
        .withColumn("event_timestamp", F.to_timestamp("timestamp"))
        .filter(F.col("price_usd").isNotNull())
        .filter(F.col("price_usd") > 0)
    )

    # ── Stream 1: Write all valid prices to postgres ──────
    prices_query = (
        parsed.writeStream
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, "crypto_prices"))
        .option("checkpointLocation", f"{CHECKPOINT}/prices")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # ── Stream 2: 1-minute windowed aggregations ──────────
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
            "open", "high", "low", "close", "avg_price", "event_count"
        )
    )

    agg_query = (
        agg_df.writeStream
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, "crypto_ohlc_1min"))
        .option("checkpointLocation", f"{CHECKPOINT}/agg")
        .trigger(processingTime="60 seconds")
        .outputMode("update")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
