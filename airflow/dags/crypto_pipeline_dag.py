"""
airflow/dags/crypto_pipeline_dag.py
─────────────────────────────────────────────────────────────
Orchestrates the crypto streaming pipeline every minute:
  1. Health-check: Kafka topic exists and is receiving data
  2. Health-check: PostgreSQL is reachable
  3. Data freshness: rows ingested in last 2 minutes
  4. Spark health: OHLC rows written in last 5 minutes
  5. Refresh OHLC stats (ANALYZE)
  6. Data quality report (null / negative / dupe checks)
─────────────────────────────────────────────────────────────
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ── Default args ──────────────────────────────────────────
default_args = {
    "owner": "nino",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# ── DAG definition ────────────────────────────────────────
with DAG(
    dag_id="crypto_streaming_pipeline",
    description="Orchestrates the real-time crypto data pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="* * * * *",  # every minute
    catchup=False,
    tags=["streaming", "crypto", "data-engineering"],
) as dag:

    # ── Task 1: Check Kafka topic via Python ──────────────
    def check_kafka_health(**kwargs):
        """
        Use the kafka-python library to verify the crypto-prices
        topic exists and has at least one partition.
        """
        from kafka import KafkaAdminClient
        from kafka.errors import KafkaError

        try:
            admin = KafkaAdminClient(
                bootstrap_servers="kafka:29092",
                client_id="airflow-health-check",
                request_timeout_ms=5000,
            )
            topics = admin.list_topics()
            admin.close()

            if "crypto-prices" not in topics:
                raise ValueError(
                    "Topic 'crypto-prices' not found in Kafka. "
                    f"Available topics: {topics}"
                )
            print(f"[Kafka Health] OK — topic 'crypto-prices' exists. "
                  f"All topics: {topics}")
        except KafkaError as e:
            raise RuntimeError(f"Kafka is unreachable: {e}")

    check_kafka = PythonOperator(
        task_id="check_kafka_health",
        python_callable=check_kafka_health,
    )

    # ── Task 2: Check data freshness in Postgres ──────────
    def check_data_freshness(**kwargs):
        hook = PostgresHook(postgres_conn_id="postgres_default")
        sql = """
            SELECT COUNT(*) FROM crypto_prices
            WHERE ingested_at >= NOW() - INTERVAL '2 minutes';
        """
        count = hook.get_first(sql)[0]
        print(f"[Data Freshness] {count} rows ingested in last 2 minutes")
        if count == 0:
            raise ValueError(
                "No data ingested in the last 2 minutes — "
                "producer or consumer may be down!"
            )
        return count

    check_freshness = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
    )

    # ── Task 3: Check Spark is producing OHLC rows ────────
    def check_spark_health(**kwargs):
        hook = PostgresHook(postgres_conn_id="postgres_default")
        sql = """
            SELECT COUNT(*) FROM crypto_ohlc_1min
            WHERE created_at >= NOW() - INTERVAL '5 minutes';
        """
        count = hook.get_first(sql)[0]
        print(f"[Spark Health] {count} OHLC rows written in last 5 minutes")
        if count == 0:
            raise ValueError(
                "No OHLC rows written in the last 5 minutes — "
                "Spark stream_processor may be down!"
            )
        return count

    check_spark = PythonOperator(
        task_id="check_spark_health",
        python_callable=check_spark_health,
    )

    # ── Task 4: Refresh OHLC stats (ANALYZE) ─────────────
    refresh_ohlc = PostgresOperator(
        task_id="refresh_ohlc_stats",
        postgres_conn_id="postgres_default",
        sql="SELECT refresh_ohlc_view();",
    )

    # ── Task 5: Data quality checks ───────────────────────
    def run_quality_checks(**kwargs):
        hook = PostgresHook(postgres_conn_id="postgres_default")

        checks = {
            "null_prices": """
                SELECT COUNT(*) FROM crypto_prices
                WHERE price_usd IS NULL
                  AND ingested_at >= NOW() - INTERVAL '1 hour';
            """,
            "negative_prices": """
                SELECT COUNT(*) FROM crypto_prices
                WHERE price_usd <= 0
                  AND ingested_at >= NOW() - INTERVAL '1 hour';
            """,
            "duplicate_events": """
                SELECT COUNT(*) FROM (
                    SELECT coin, event_timestamp
                    FROM crypto_prices
                    WHERE ingested_at >= NOW() - INTERVAL '1 hour'
                    GROUP BY coin, event_timestamp
                    HAVING COUNT(*) > 1
                ) AS dupes;
            """,
            "coins_reporting": """
                SELECT COUNT(DISTINCT coin) FROM crypto_prices
                WHERE ingested_at >= NOW() - INTERVAL '1 hour';
            """,
            "anomalies_last_hour": """
                SELECT COUNT(*) FROM crypto_anomalies
                WHERE flagged_at >= NOW() - INTERVAL '1 hour';
            """,
        }

        results = {}
        for check_name, sql in checks.items():
            result = hook.get_first(sql)[0]
            results[check_name] = result
            print(f"[Quality Check] {check_name}: {result}")

        if results["null_prices"] > 0 or results["negative_prices"] > 0:
            raise ValueError(f"Data quality failure: {results}")

        expected_coins = 5  # BTC, ETH, SOL, ADA, DOGE
        if results["coins_reporting"] < expected_coins:
            raise ValueError(
                f"Only {results['coins_reporting']}/{expected_coins} coins "
                "reporting — some feeds may be missing!"
            )

        print(f"[Quality Check] All checks passed. Results: {results}")
        return results

    quality_checks = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality_checks,
    )

    # ── Task dependencies ─────────────────────────────────
    # check_kafka ──► check_freshness ──► check_spark ──► refresh_ohlc ──► quality_checks
    check_kafka >> check_freshness >> check_spark >> refresh_ohlc >> quality_checks
