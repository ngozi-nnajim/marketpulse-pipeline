-- MarketPulse dashboard queries (Metabase native SQL questions).
-- Times are UTC.

-- 1. Market overview: latest price and change today, per symbol.
-- Visualization: Table
SELECT
    symbol,
    asset_class,
    (array_agg(close ORDER BY bar_timestamp DESC))[1] AS latest_price,
    ROUND(
        100 * ((array_agg(close ORDER BY bar_timestamp DESC))[1]
             / (array_agg(close ORDER BY bar_timestamp ASC))[1] - 1),
        2
    ) AS pct_change_today,
    SUM(volume) AS volume_today,
    MAX(bar_timestamp) AS last_update
FROM raw_prices
WHERE bar_timestamp >= date_trunc('day', now())
GROUP BY symbol, asset_class
ORDER BY asset_class DESC, symbol;

-- 2. Stock price trend over the last 3 hours, as % change since the first
-- bar in the window, so all stocks share one scale despite different prices.
-- Visualization: Line chart (x = bar_timestamp, y = pct_change, series = symbol)
SELECT
    bar_timestamp,
    symbol,
    ROUND(100 * (close / FIRST_VALUE(close) OVER (
        PARTITION BY symbol ORDER BY bar_timestamp) - 1), 3) AS pct_change
FROM raw_prices
WHERE asset_class = 'stock'
  AND bar_timestamp >= now() - INTERVAL '3 hours'
ORDER BY bar_timestamp;

-- 3. Trading volume per symbol today.
-- Visualization: Bar chart (x = symbol, y = total_volume)
SELECT symbol, SUM(volume) AS total_volume
FROM raw_prices
WHERE asset_class = 'stock'
  AND bar_timestamp >= date_trunc('day', now())
GROUP BY symbol
ORDER BY total_volume DESC;

-- 4. Volatility: price range within each 5-minute window, as a % of the
-- average price, so cheap and expensive assets are comparable.
-- Visualization: Line chart (x = window_start, y = price_range_pct, series = symbol)
SELECT
    window_start,
    symbol,
    ROUND(100 * (max_high - min_low) / avg_close, 3) AS price_range_pct
FROM stock_metrics
WHERE window_start >= now() - INTERVAL '3 hours'
ORDER BY window_start;

-- 5. Pipeline latency: seconds from the producer receiving a bar to it being
-- stored in PostgreSQL (time spent in Kafka + Spark).
-- Visualization: Line chart (x = minute, y = avg_latency_seconds)
SELECT
    date_trunc('minute', processed_at) AS minute,
    ROUND(AVG(EXTRACT(EPOCH FROM processed_at - ingested_at))::numeric, 2)
        AS avg_latency_seconds,
    COUNT(*) AS bars_processed
FROM raw_prices
WHERE processed_at >= now() - INTERVAL '3 hours'
GROUP BY 1
ORDER BY 1;

-- 6. Data quality: rejected records by reason.
-- Visualization: Table or Bar chart
SELECT reason, COUNT(*) AS rejected_count, MAX(rejected_at) AS last_seen
FROM rejected_records
GROUP BY reason
ORDER BY rejected_count DESC;
