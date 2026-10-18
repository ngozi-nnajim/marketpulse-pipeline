-- Database schema for the MarketPulse pipeline.
-- Runs automatically ONLY when the Postgres volume is first created.
-- To re-run after editing: docker compose down -v && docker compose up -d
-- (warning: -v deletes all stored data).

-- Validated one-minute bars, one row per symbol per minute.
CREATE TABLE IF NOT EXISTS raw_prices (
    symbol        TEXT          NOT NULL,
    asset_class   TEXT          NOT NULL CHECK (asset_class IN ('stock', 'crypto')),
    bar_timestamp TIMESTAMPTZ   NOT NULL,  -- START of the 1-minute bar, in UTC
    -- NUMERIC stores exact decimals. Floats can introduce tiny rounding
    -- errors, which is unacceptable for financial data.
    open          NUMERIC(18,6) NOT NULL,
    high          NUMERIC(18,6) NOT NULL,
    low           NUMERIC(18,6) NOT NULL,
    close         NUMERIC(18,6) NOT NULL,
    volume        NUMERIC(24,8) NOT NULL,  -- Fractional, because crypto trades in fractions
    trade_count   INTEGER       NOT NULL,
    vwap          NUMERIC(18,6),           -- Nullable: not guaranteed in every message
    ingested_at   TIMESTAMPTZ   NOT NULL,  -- When the producer received the bar
    processed_at  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    -- The primary key forbids duplicates. If the pipeline restarts and
    -- re-sends a bar, it can be safely ignored instead of double-counting.
    -- This is to ensure idempotency.
    PRIMARY KEY (symbol, bar_timestamp)
);

-- 5-minute aggregates computed by Spark's streaming windows.
CREATE TABLE IF NOT EXISTS stock_metrics (
    symbol        TEXT          NOT NULL,
    window_start  TIMESTAMPTZ   NOT NULL,
    window_end    TIMESTAMPTZ   NOT NULL,
    avg_close     NUMERIC(18,6) NOT NULL,
    min_low       NUMERIC(18,6) NOT NULL,
    max_high      NUMERIC(18,6) NOT NULL,
    total_volume  NUMERIC(24,8) NOT NULL,
    bar_count     INTEGER       NOT NULL,
    processed_at  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, window_start)
);

-- Messages that failed validation. Keeping the original payload so the
-- problem can be investigated, and the data fixed and replayed if needed.
CREATE TABLE IF NOT EXISTS rejected_records (
    id            BIGSERIAL   PRIMARY KEY,
    raw_payload   TEXT        NOT NULL,
    reason        TEXT        NOT NULL,
    rejected_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Dashboards mostly ask for "the latest data", so index by time.
CREATE INDEX IF NOT EXISTS idx_raw_prices_bar_timestamp
    ON raw_prices (bar_timestamp DESC);
