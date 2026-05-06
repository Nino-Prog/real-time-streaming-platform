# Real-Time Crypto Streaming Platform

A production-grade, end-to-end real-time data engineering pipeline built entirely on free, open-source tools. Streams live cryptocurrency prices from the CoinGecko API through Apache Kafka, processes them with Apache Spark, stores them in PostgreSQL, orchestrates workflows with Apache Airflow, and visualizes everything in Grafana — all running locally via Docker.

---

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────────────┐
│                 │     │                 │     │                         │
│  CoinGecko API  │────▶│  Python         │────▶│  Apache Kafka           │
│  (Free, no key) │     │  Producer       │     │  Topic: crypto-prices   │
│                 │     │                 │     │                         │
└─────────────────┘     └─────────────────┘     └────────────┬────────────┘
                                                              │
                                          ┌───────────────────┼───────────────────┐
                                          │                   │                   │
                                          ▼                   ▼                   │
                                 ┌─────────────────┐ ┌─────────────────┐         │
                                 │  Python         │ │  Apache Spark   │         │
                                 │  Consumer       │ │  Structured     │         │
                                 │  (raw ingest)   │ │  Streaming      │         │
                                 └────────┬────────┘ └────────┬────────┘         │
                                          │                   │                   │
                                          └─────────┬─────────┘                   │
                                                    ▼                             │
                                         ┌─────────────────┐                     │
                                         │                 │                     │
                                         │   PostgreSQL    │                     │
                                         │   - raw prices  │                     │
                                         │   - OHLC view   │                     │
                                         │   - anomalies   │                     │
                                         │                 │                     │
                                         └────────┬────────┘                     │
                                                  │                              │
                              ┌───────────────────┼──────────────────┐           │
                              │                   │                  │           │
                              ▼                   ▼                  ▼           │
                     ┌─────────────────┐ ┌─────────────────┐        │           │
                     │    Grafana      │ │    Airflow      │◀────────┘           │
                     │   Dashboard     │ │  Orchestration  │   monitors pipeline │
                     │  (live charts)  │ │  & Data Quality │                     │
                     └─────────────────┘ └─────────────────┘                     │
```

---

## Stack

| Layer | Tool | Version |
|---|---|---|
| Message Broker | Apache Kafka | 7.5.0 |
| Stream Processing | Apache Spark Structured Streaming | 3.5.0 |
| Storage | PostgreSQL | 15 |
| Orchestration | Apache Airflow | 2.8.0 |
| Visualization | Grafana | 10.2.0 |
| Containerization | Docker + Docker Compose | Latest |
| Language | Python | 3.11+ |

---

## Quick Start

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (16GB RAM recommended)
- Python 3.11+
- Git

### 1. Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/real-time-streaming-platform.git
cd real-time-streaming-platform
```

### 2. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 3. Start all services
```bash
docker-compose up -d
```

Wait ~60 seconds for all services to initialize.

### 4. Verify services are running
```bash
docker-compose ps
```

All services should show `Up`.

### 5. Start the Kafka producer (in a new terminal)
```bash
python producer/crypto_producer.py
```

### 6. Start the Kafka consumer (in a new terminal)
```bash
python consumer/crypto_consumer.py
```

### 7. Submit the Spark job (in a new terminal)
```bash
docker exec spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
  /spark_jobs/stream_processor.py
```

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Grafana Dashboard | http://localhost:3000 | admin / admin |
| Airflow UI | http://localhost:8081 | admin / admin |
| Spark Master UI | http://localhost:8080 | — |

---

## Project Structure

```
real-time-streaming-platform/
├── docker-compose.yml          # Spins up entire stack
├── requirements.txt            # Python dependencies
├── README.md
│
├── producer/
│   └── crypto_producer.py      # Pulls CoinGecko API → publishes to Kafka
│
├── consumer/
│   └── crypto_consumer.py      # Reads Kafka → writes to PostgreSQL
│
├── spark_jobs/
│   └── stream_processor.py     # Spark Structured Streaming + anomaly detection
│
├── airflow/
│   └── dags/
│       └── crypto_pipeline_dag.py  # Orchestration + data quality checks
│
├── sql/
│   └── init.sql                # PostgreSQL schema + materialized views
│
└── grafana/
    └── dashboards/             # Grafana dashboard JSON configs
```

---

## Data Flow

1. **Producer** polls CoinGecko every 10 seconds for BTC, ETH, SOL, ADA, DOGE prices
2. Each price event is published to the `crypto-prices` Kafka topic as JSON
3. **Consumer** reads events and writes raw data to the `crypto_prices` PostgreSQL table
4. **Spark** reads the same topic, computes 1-minute OHLC windows, and flags anomalies
5. **Airflow** runs every minute to refresh materialized views and run data quality checks
6. **Grafana** queries PostgreSQL to display live price charts and anomaly alerts

---

## Key Features

- ✅ Real-time ingestion with sub-second Kafka latency
- ✅ OHLC (Open/High/Low/Close) aggregations via Spark windowing
- ✅ Anomaly detection — flags prices deviating >10% from 1-hour rolling average
- ✅ Data quality checks in Airflow (nulls, duplicates, freshness)
- ✅ Optimized PostgreSQL indexes for fast time-series queries
- ✅ Fully containerized — runs on any machine with Docker
- ✅ Zero cloud costs — 100% local

---

## Author

**Nino Ombongi**
nino.ombongi.work@gmail.com | Durham, NC
