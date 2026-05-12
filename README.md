# Real-Time Crypto Streaming Platform

A production-grade, end-to-end real-time data engineering pipeline built entirely on free, open-source tools. Streams live cryptocurrency prices from the CoinGecko API through Apache Kafka, processes them with Apache Spark Structured Streaming, stores results in PostgreSQL, orchestrates everything with Apache Airflow, and visualizes it in Grafana — all running locally via Docker with a single command.

---

## Architecture

```
┌──────────────────┐
│  CoinGecko API   │  (free, no API key)
│  BTC ETH SOL     │
│  ADA DOGE        │
└────────┬─────────┘
         │ HTTP poll every 10s
         ▼
┌──────────────────┐        ┌───────────────────────────┐
│  Python Producer │──────▶ │  Apache Kafka             │
│  (Dockerized)    │        │  topic: crypto-prices     │
└──────────────────┘        └─────────────┬─────────────┘
                                           │
                          ┌────────────────┼────────────────┐
                          ▼                                  ▼
             ┌────────────────────┐         ┌────────────────────────────┐
             │  Python Consumer   │         │  Apache Spark              │
             │  (Dockerized)      │         │  Structured Streaming      │
             │  raw ingest        │         │                            │
             │  + validation      │         │  Stream 1 → crypto_prices  │
             └─────────┬──────────┘         │  Stream 2 → crypto_ohlc_1min│
                       │                    │  Stream 3 → crypto_anomalies│
                       │                    └────────────┬───────────────┘
                       │                                 │
                       └────────────────┬────────────────┘
                                        ▼
                             ┌──────────────────┐
                             │   PostgreSQL      │
                             │  crypto_prices    │  ← raw events
                             │  crypto_ohlc_1min │  ← Spark OHLC
                             │  crypto_anomalies │  ← Spark anomalies
                             └────────┬──────────┘
                                      │
                     ┌────────────────┼────────────────┐
                     ▼                                  ▼
          ┌──────────────────┐             ┌──────────────────┐
          │     Grafana      │             │  Apache Airflow  │
          │  Live Dashboard  │             │  Orchestration   │
          │  - Price charts  │             │  - Kafka health  │
          │  - OHLC table    │             │  - Data freshness│
          │  - Anomaly feed  │             │  - Spark health  │
          │  - 24h change %  │             │  - Quality checks│
          └──────────────────┘             └──────────────────┘
```

---

## Stack

| Layer | Tool | Version |
|---|---|---|
| Message Broker | Apache Kafka | 7.5.0 (Confluent) |
| Stream Processing | Apache Spark Structured Streaming | 3.5 |
| Storage | PostgreSQL | 15 |
| Orchestration | Apache Airflow | 2.8.0 |
| Visualization | Grafana | 10.2.0 |
| Containerization | Docker + Docker Compose | — |
| Language | Python | 3.11 |
| Data Source | CoinGecko API | Free, no key needed |

---

## Quick Start

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (16 GB RAM recommended)
- Git

### 1. Clone the repo
```bash
git clone https://github.com/Nino-Prog/real-time-streaming-platform.git
cd real-time-streaming-platform
```

### 2. Start the entire stack
```bash
docker-compose up --build
```

That's it. Docker Compose starts all 9 services in the correct order:
`zookeeper → kafka → postgres → producer → consumer → spark-master → spark-worker → spark-submit → airflow → grafana`

Wait ~60 seconds for everything to initialize, then open the dashboards:

| Service | URL | Credentials |
|---|---|---|
| **Grafana Dashboard** | http://localhost:3000 | admin / admin |
| **Airflow UI** | http://localhost:8081 | admin / admin |
| **Spark Master UI** | http://localhost:8080 | — |

### 3. Verify data is flowing
```bash
# Check all services are healthy
docker-compose ps

# Watch live Kafka messages
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic crypto-prices --from-beginning

# Query PostgreSQL directly
docker exec -it postgres psql -U streamuser -d streamdb \
  -c "SELECT coin, price_usd, ingested_at FROM crypto_prices ORDER BY ingested_at DESC LIMIT 10;"
```

---

## How It Works

### Data Flow

1. **Producer** polls CoinGecko every 10 s for BTC, ETH, SOL, ADA, DOGE prices
2. Each price event is published to the `crypto-prices` Kafka topic as JSON
3. **Consumer** reads events, validates them, and writes raw rows to `crypto_prices`
4. **Spark** runs 3 concurrent streams from the same Kafka topic:
   - **Stream 1** — writes all valid prices to `crypto_prices` (every 10 s)
   - **Stream 2** — computes 1-minute OHLC windows → `crypto_ohlc_1min` (every 60 s)
   - **Stream 3** — detects prices deviating >10% from 1h rolling average → `crypto_anomalies` (every 30 s)
5. **Airflow** runs every minute: checks Kafka health, data freshness, Spark output, and data quality
6. **Grafana** auto-refreshes every 10 s, pulling live data from PostgreSQL

### Anomaly Detection

Spark's anomaly stream reads the per-coin 1-hour average directly from Postgres via JDBC, joins it with the current micro-batch, and writes any row with `deviation_pct > 10%` to `crypto_anomalies`. The Grafana dashboard surfaces these in real time with colour-coded severity.

---

## Project Structure

```
real-time-streaming-platform/
├── docker-compose.yml              # Orchestrates all 9 services
├── requirements.txt                # Python dependencies (local dev)
│
├── producer/
│   ├── Dockerfile                  # Containerized producer image
│   └── crypto_producer.py          # CoinGecko API → Kafka
│
├── consumer/
│   ├── Dockerfile                  # Containerized consumer image
│   └── crypto_consumer.py          # Kafka → PostgreSQL (raw ingest)
│
├── spark_jobs/
│   └── stream_processor.py         # Spark Structured Streaming
│                                   # (3 streams: prices, OHLC, anomalies)
│
├── airflow/
│   └── dags/
│       └── crypto_pipeline_dag.py  # Pipeline health + quality checks
│
├── sql/
│   └── init.sql                    # Schema: crypto_prices,
│                                   # crypto_ohlc_1min, crypto_anomalies
│
└── grafana/
    ├── provisioning/
    │   ├── datasources/
    │   │   └── postgres.yml        # Auto-wires PostgreSQL datasource
    │   └── dashboards/
    │       └── crypto.yml          # Dashboard provider config
    └── dashboards/
        └── crypto_dashboard.json   # 11-panel live dashboard
```

---

## Dashboard Panels

| Panel | Type | Source |
|---|---|---|
| BTC / ETH / SOL / ADA / DOGE latest price | Stat cards | `crypto_prices` |
| Anomaly alert count (last hour) | Stat card | `crypto_anomalies` |
| Live price chart (all 5 coins) | Time series | `crypto_prices` |
| 1-minute OHLC aggregations | Table | `crypto_ohlc_1min` (Spark) |
| Price anomalies feed | Table | `crypto_anomalies` (Spark) |
| 24h change % by coin | Bar chart | `crypto_prices` |
| Event ingestion rate (events/min) | Bar time series | `crypto_prices` |

---

## Key Features

- **Real-time ingestion** — sub-second Kafka latency, 10-second poll cycle
- **3 concurrent Spark streams** — raw prices, OHLC aggregations, anomaly detection
- **Anomaly detection** — flags any price deviating >10% from its 1-hour average
- **Automated orchestration** — Airflow checks Kafka, Postgres, and Spark health every minute
- **Zero config startup** — `docker-compose up --build` wires everything automatically
- **Portfolio-ready** — full Docker setup, documented schema, provisioned Grafana dashboard
- **Zero cloud costs** — 100% local, no API keys, no paid services

---

## Author

**Nino Ombongi**
nino.ombongi.work@gmail.com

[GitHub](https://github.com/Nino-Prog) | [LinkedIn](https://www.linkedin.com/in/nino-ombongi-027325254/)
