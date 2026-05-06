-- sql/init.sql
-- ─────────────────────────────────────────────────────────
-- Initializes the streamdb schema for the crypto pipeline.
-- Runs automatically when the PostgreSQL container starts.
-- ─────────────────────────────────────────────────────────

-- Raw price events (written by consumer)
CREATE TABLE IF NOT EXISTS crypto_prices (
    id               SERIAL PRIMARY KEY,
    coin             VARCHAR(50)    NOT NULL,
    price_usd        NUMERIC(20, 8) NOT NULL,
    market_cap_usd   NUMERIC(30, 2),
    volume_24h_usd   NUMERIC(30, 2),
    change_24h_pct   NUMERIC(10, 4),
    event_timestamp  TIMESTAMPTZ    NOT NULL,
    ingested_at      TIMESTAMPTZ    DEFAULT NOW()
);

-- Index for fast queries by coin and time
CREATE INDEX IF NOT EXISTS idx_crypto_coin       ON crypto_prices(coin);
CREATE INDEX IF NOT EXISTS idx_crypto_timestamp  ON crypto_prices(event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_crypto_coin_time  ON crypto_prices(coin, event_timestamp DESC);

-- ─────────────────────────────────────────────────────────
-- Aggregated 1-minute OHLC view (used by Grafana dashboard)
-- ─────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS crypto_ohlc_1min AS
SELECT
    coin,
    date_trunc('minute', event_timestamp)               AS bucket,
    FIRST_VALUE(price_usd) OVER w                       AS open,
    MAX(price_usd)         OVER w                       AS high,
    MIN(price_usd)         OVER w                       AS low,
    LAST_VALUE(price_usd)  OVER w                       AS close,
    AVG(price_usd)         OVER w                       AS avg_price,
    SUM(volume_24h_usd)    OVER w                       AS total_volume,
    COUNT(*)               OVER w                       AS event_count
FROM crypto_prices
WINDOW w AS (
    PARTITION BY coin, date_trunc('minute', event_timestamp)
    ORDER BY event_timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
)
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_ohlc_coin_bucket
    ON crypto_ohlc_1min(coin, bucket);

-- ─────────────────────────────────────────────────────────
-- Anomaly flags table (written by Spark job)
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS crypto_anomalies (
    id              SERIAL PRIMARY KEY,
    coin            VARCHAR(50)    NOT NULL,
    price_usd       NUMERIC(20, 8) NOT NULL,
    avg_price_1h    NUMERIC(20, 8),
    deviation_pct   NUMERIC(10, 4),
    flagged_at      TIMESTAMPTZ    DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────
-- Helper function to refresh the materialized view
-- (called by Airflow DAG every minute)
-- ─────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION refresh_ohlc_view()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY crypto_ohlc_1min;
END;
$$ LANGUAGE plpgsql;
