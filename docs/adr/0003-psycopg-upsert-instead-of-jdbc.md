# ADR 0003: Write to PostgreSQL with psycopg upserts instead of Spark's JDBC writer

**Status:** Accepted
**Date:** 2026-10-19

## Context
Spark Structured Streaming guarantees **at-least-once** processing: after a
crash or restart, it replays any micro-batch whose completion it hadn't
recorded in its checkpoint. A replayed batch therefore writes rows that may
already exist in PostgreSQL.

The `raw_prices` and `stock_metrics` tables use primary keys
(`symbol, bar_timestamp` and `symbol, window_start`) to prevent duplicate
rows. Spark's built-in JDBC writer can only perform plain `INSERT`s, so a
replayed batch would violate the primary key, fail, and crash the stream.

## Options considered
1. **Spark's JDBC writer (append mode):** simple and parallel, but cannot skip
   duplicates, so replays crash the job.
2. **JDBC into a staging table, then an SQL merge:** handles duplicates and
   scales to large volumes, but adds a staging table and an extra step.
3. **psycopg inside `foreachBatch`, with `INSERT ... ON CONFLICT DO NOTHING`:**
   each micro-batch is collected to the Spark driver and inserted with the
   standard Python PostgreSQL driver; duplicates are skipped silently.

## Decision
Use psycopg upserts inside `foreachBatch` (option 3).

## Consequences
- **Positive:** writes are **idempotent**. Replays are harmless, which was
  proven in practice: after a failed batch, Spark retried it, and the rows
  already written were skipped instead of crashing the stream.
- **Positive:** simple to implement, and to reason about.
- **Negative:** each batch is collected to the driver, which only suits small
  batches. Our volume is a few bars per minute, so this is fine here; at
  large scale, option 2 would be the standard approach.
- **Negative:** `rejected_records` has no natural key, so a replayed batch
  can store the same rejection twice. This is acceptable for an error log,
  and documented as a known limitation.
- **Note:** the architecture diagram labels this connection
  "Upsert (foreachBatch)" rather than "JDBC Write".
