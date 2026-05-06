"""
airflow/dags/crypto_pipeline_dag.py
─────────────────────────────────────────────────────────────
Orchestrates the crypto streaming pipeline:
  1. Health-checks Kafka and PostgreSQL
  2. Refreshes the OHLC materialized view every minute
  3. Runs a data quality report every hour
─────────────────────────────────────────────────────────────
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
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
    start_date=datetime(2025, 1, 1),
    schedule_interval="* * * * *",  # every minute
    catchup=False,
    tags=["streaming", "crypto", "data-engineering"],
) as dag:

    # ── Task 1: Check Kafka is reachable ──────────────────
    check_kafka = BashOperator(
        task_id="check_kafka_health",
        bash_command=(
            "kafka-topics.sh --bootstrap-server kafka:29092 --list "
            "| grep -q 'crypto-prices' && echo 'Kafka OK' || exit 1"
        ),
    )

    # ── Task 2: Check row count in last 2 minutes ─────────
    def check_data_freshness(**context):
        hook = PostgresHook(postgres_conn_id="postgres_default")
        sql = """
            SELECT COUNT(*) FROM crypto_prices
            WHERE ingested_at >= NOW() - INTERVAL '2 minutes';
        """
        count = hook.get_first(sql)[0]
        print(f"[Data Freshness] {count} rows ingested in last 2 minutes")
        if count == 0:
            raise ValueError("No data ingested in the last 2 minutes — pipeline may be down!")
        return count

    check_freshness = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
        provide_context=True,
    )

    # ── Task 3: Refresh OHLC materialized view ────────────
    refresh_ohlc = PostgresOperator(
        task_id="refresh_ohlc_view",
        postgres_conn_id="postgres_default",
        sql="SELECT refresh_ohlc_view();",
    )

    # ── Task 4: Data quality summary (runs every hour) ────
    def run_quality_checks(**context):
        hook = PostgresHook(postgres_conn_id="postgres_default")

        checks = {
            "null_prices": """
                SELECT COUNT(*) FROM crypto_prices
                WHERE price_usd IS NULL AND ingested_at >= NOW() - INTERVAL '1 hour';
            """,
            "negative_prices": """
                SELECT COUNT(*) FROM crypto_prices
                WHERE price_usd <= 0 AND ingested_at >= NOW() - INTERVAL '1 hour';
            """,
            "duplicate_events": """
                SELECT COUNT(*) FROM (
                    SELECT coin, event_timestamp, COUNT(*)
                    FROM crypto_prices
                    WHERE ingested_at >= NOW() - INTERVAL '1 hour'
                    GROUP BY coin, event_timestamp
                    HAVING COUNT(*) > 1
                ) dupes;
            """,
            "coins_reporting": """
                SELECT COUNT(DISTINCT coin) FROM crypto_prices
                WHERE ingested_at >= NOW() - INTERVAL '1 hour';
            """,
        }

        results = {}
        for check_name, sql in checks.items():
            result = hook.get_first(sql)[0]
            results[check_name] = result
            print(f"[Quality Check] {check_name}: {result}")

        # Fail if critical issues found
        if results["null_prices"] > 0 or results["negative_prices"] > 0:
            raise ValueError(f"Data quality failure: {results}")

        print(f"[Quality Check] All checks passed. Results: {results}")
        return results

    quality_checks = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality_checks,
        provide_context=True,
    )

    # ── Task dependencies ─────────────────────────────────
    check_kafka >> check_freshness >> refresh_ohlc >> quality_checks
