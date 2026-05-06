"""
producer/crypto_producer.py
─────────────────────────────────────────────────────────────
Pulls live crypto prices from CoinGecko (free, no API key needed)
and publishes each price event to a Kafka topic.

Run: python producer/crypto_producer.py
"""

import json
import os
import time
import logging
from datetime import datetime

import requests
from kafka import KafkaProducer

# ── Config (env vars allow override in docker-compose) ────
KAFKA_BROKER          = os.getenv("KAFKA_BROKER",         "localhost:9092")
TOPIC                 = os.getenv("KAFKA_TOPIC",          "crypto-prices")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "10"))

COINS = [
    "bitcoin",
    "ethereum",
    "solana",
    "cardano",
    "dogecoin",
]

COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/simple/price"
    "?ids={ids}&vs_currencies=usd&include_24hr_change=true"
    "&include_market_cap=true&include_24hr_vol=true"
)

# ── Logging ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)


# ── Kafka setup ───────────────────────────────────────────
def create_producer() -> KafkaProducer:
    """Create and return a KafkaProducer with JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        retries=5,
        acks="all",
    )


# ── Fetch prices ──────────────────────────────────────────
def fetch_prices() -> dict:
    """Fetch latest prices from CoinGecko for all tracked coins."""
    url = COINGECKO_URL.format(ids=",".join(COINS))
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


# ── Build message ─────────────────────────────────────────
def build_event(coin: str, data: dict) -> dict:
    """Build a structured event payload for a single coin."""
    coin_data = data.get(coin, {})
    return {
        "coin": coin,
        "price_usd": coin_data.get("usd"),
        "market_cap_usd": coin_data.get("usd_market_cap"),
        "volume_24h_usd": coin_data.get("usd_24h_vol"),
        "change_24h_pct": coin_data.get("usd_24h_change"),
        "timestamp": datetime.utcnow().isoformat(),
    }


# ── Main loop ─────────────────────────────────────────────
def main():
    log.info("Starting crypto producer...")
    producer = create_producer()
    log.info(f"Connected to Kafka at {KAFKA_BROKER}")

    while True:
        try:
            raw_data = fetch_prices()
            log.info(f"Fetched data for {len(raw_data)} coins")

            for coin in COINS:
                event = build_event(coin, raw_data)
                producer.send(TOPIC, key=coin, value=event)
                log.info(f"Published → {coin}: ${event['price_usd']:,.2f}")

            producer.flush()

        except requests.RequestException as e:
            log.error(f"API fetch failed: {e}")
        except Exception as e:
            log.error(f"Unexpected error: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
