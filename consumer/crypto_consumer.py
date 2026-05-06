"""
consumer/crypto_consumer.py
─────────────────────────────────────────────────────────────
Reads crypto price events from Kafka and writes them to
the PostgreSQL `crypto_prices` table.

Run: python consumer/crypto_consumer.py
"""

import json
import logging
import os
import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer

# ── Config (env vars allow override in docker-compose) ────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC        = os.getenv("KAFKA_TOPIC",  "crypto-prices")
GROUP_ID     = os.getenv("GROUP_ID",     "crypto-consumer-group")

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "dbname":   os.getenv("DB_NAME",     "streamdb"),
    "user":     os.getenv("DB_USER",     "streamuser"),
    "password": os.getenv("DB_PASSWORD", "streampass"),
}

# ── Logging ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)


# ── DB connection ─────────────────────────────────────────
def get_db_connection():
    """Create and return a PostgreSQL connection."""
    return psycopg2.connect(**DB_CONFIG)


# ── Insert event ──────────────────────────────────────────
INSERT_SQL = """
    INSERT INTO crypto_prices (
        coin, price_usd, market_cap_usd,
        volume_24h_usd, change_24h_pct, event_timestamp
    ) VALUES %s
    ON CONFLICT DO NOTHING;
"""

def insert_event(conn, event: dict):
    """Insert a single price event into PostgreSQL."""
    row = (
        event["coin"],
        event["price_usd"],
        event["market_cap_usd"],
        event["volume_24h_usd"],
        event["change_24h_pct"],
        event["timestamp"],
    )
    with conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, [row])
    conn.commit()


# ── Data quality check ────────────────────────────────────
def is_valid(event: dict) -> bool:
    """Basic data quality check before writing to DB."""
    required_fields = ["coin", "price_usd", "timestamp"]
    for field in required_fields:
        if event.get(field) is None:
            log.warning(f"Missing field '{field}' in event: {event}")
            return False
    if event["price_usd"] <= 0:
        log.warning(f"Invalid price for {event['coin']}: {event['price_usd']}")
        return False
    return True


# ── Main loop ─────────────────────────────────────────────
def main():
    log.info("Starting crypto consumer...")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    log.info(f"Subscribed to topic: {TOPIC}")

    conn = get_db_connection()
    log.info("Connected to PostgreSQL")

    for message in consumer:
        event = message.value

        if not is_valid(event):
            continue

        try:
            insert_event(conn, event)
            log.info(
                f"Stored → {event['coin']}: "
                f"${event['price_usd']:,.2f} at {event['timestamp']}"
            )
        except Exception as e:
            log.error(f"DB insert failed: {e}")
            conn = get_db_connection()  # reconnect on failure


if __name__ == "__main__":
    main()
