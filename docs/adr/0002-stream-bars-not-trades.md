# ADR 0002: Stream one-minute bars rather than individual trades

**Status:** Accepted
**Date:** 2026-10-18

## Context
Alpaca's WebSocket can stream either individual **trades** (every trade as it
happens) or one-minute **bars** (a summary of each minute: open, high, low,
close, volume, trade count, VWAP). The choice shapes the Kafka message format,
the Spark processing logic, and the database schema.

The project had a strict deadline, and several components (Kafka, Spark, PostgreSQL, Metabase, tests, and CI) were still to be built when this
decision was made.

## Options considered
1. **Stream trades:** sub-second latency and high message volume. Spark would
   build the bars itself from raw trades, using time windows and watermarks
   to handle late-arriving trades.
2. **Stream bars:** a few messages per symbol per minute. Alpaca builds the
   bars; Spark validates them and computes further metrics on top.

## Decision
Stream one-minute bars (option 2).

## Consequences
- **Positive:** a simpler, lower-risk implementation that fit the deadline,
  while still demonstrating real streaming: Kafka partitioning, Spark
  micro-batches, checkpointing, and windowed aggregation with a watermark
  (the 5-minute metrics).
- **Positive:** low message volume keeps local resource usage small.
- **Negative:** data granularity is one minute. A price move is visible only
  once its minute closes, so end-to-end freshness is up to about a minute,
  even though the pipeline itself processes each bar within seconds.
- **Negative:** fewer events to stress-test throughput and scalability.
- **Future work:** switching to trades requires a new message schema
  (versioned via `schema_version`) and a Spark job that aggregates trades
  into bars. The rest of the architecture stays the same.
