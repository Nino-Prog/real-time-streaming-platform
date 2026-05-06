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
-- 1-minute OHLC table (written directly by Spark job)
-- Spark uses foreachBatch + JDBC append — must be a plain
-- table, not a materialized view.
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS crypto_ohlc_1min (
    id          SERIAL PRIMARY KEY,
    coin        VARCHAR(50)    NOT NULL,
    bucket      TIMESTAMPTZ    NOT NULL,
    open        NUMERIC(20, 8),
    high        NUMERIC(20, 8),
    low         NUMERIC(20, 8),
    close       NUMERIC(20, 8),
    avg_price   NUMERIC(20, 8),
    event_count BIGINT,
    created_at  TIMESTAMPTZ    DEFAULT NOW(),
    UNIQUE (coin, bucket)
);

CREATE INDEX IF NOT EXISTS idx_ohlc_coin_bucket
    ON crypto_ohlc_1min(coin, bucket DESC);

-- ─────────────────────────────────────────────────────────
-- Anomaly flags table (written by Spark job)
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS crypto_anomalies (
    id              SERIAL PRIMARY KEY,
    coin            VARCHAR(50)    NOT NULL,
    price_usd       NUMERIC(20, 8) NOT NULL,
    avg_price_1h    NUMERIC(20, 8),
    deviation_pct   NUMERIC(10, 4),
    event_timestamp TIMESTAMPTZ,
    flagged_at      TIMESTAMPTZ    DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_anomalies_coin
    ON crypto_anomalies(coin, flagged_at DESC);

-- ─────────────────────────────────────────────────────────
-- Helper: called by Airflow to run a quick ANALYZE so
-- the query planner stays up-to-date on the OHLC table.
-- ─────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION refresh_ohlc_view()
RETURNS VOID AS $$
BEGIN
    ANALYZE crypto_ohlc_1min;
END;
$$ LANGUAGE plpgsql;
