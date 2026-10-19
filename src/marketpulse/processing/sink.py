"""Write micro-batches from Spark into PostgreSQL.

Uses psycopg with INSERT ... ON CONFLICT DO NOTHING, so replayed batches
(e.g. after a restart) skip duplicates instead of crashing the stream.
Spark's built-in JDBC writer can only plain-insert, so it can't do this.
"""

# Spark's image runs Python 3.8. This makes Python treat type hints as plain
# text instead of evaluating them, so modern hints like list[str] and
# `X | None` don't crash on older versions.
from __future__ import annotations

from datetime import datetime, timezone

import psycopg
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

RAW_PRICE_COLUMNS = [
    "symbol",
    "asset_class",
    "bar_timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_count",
    "vwap",
    "ingested_at",
]

METRIC_COLUMNS = [
    "symbol",
    "window_start",
    "window_end",
    "avg_close",
    "min_low",
    "max_high",
    "total_volume",
    "bar_count",
]


def _as_utc(value: object) -> object:
    """Mark naive datetimes as UTC; leave other values unchanged.

    PySpark returns timestamps without timezone info. Labelling them
    explicitly avoids PostgreSQL guessing the timezone.
    """
    if isinstance(value, datetime) and value.tzinfo is None:
        # Spark's image runs Python 3.8, which lacks datetime.UTC (added in 3.11).
        return value.replace(tzinfo=timezone.utc)  # noqa: UP017
    return value


def _collect_rows(df: DataFrame, columns: list[str]) -> list[tuple]:
    """Pull a (small) micro-batch to the driver as tuples, UTC-labelled."""
    return [
        tuple(_as_utc(row[c]) for c in columns)
        for row in df.select(*columns).toLocalIterator()
    ]


def _insert(conninfo: str, table: str, columns: list[str], rows: list) -> None:
    """Insert rows, silently skipping any that already exist."""
    if not rows:
        return
    placeholders = ", ".join(["%s"] * len(columns))
    sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) "
        f"VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    )
    # The connection context manager commits on success, rolls back on error.
    with psycopg.connect(conninfo) as conn, conn.cursor() as cur:
        cur.executemany(sql, rows)


def write_raw_batch(batch_df: DataFrame, conninfo: str) -> None:
    """Write valid bars to raw_prices and invalid ones to rejected_records.

    Args:
        batch_df: One micro-batch, including the `rejection_reason` column.
        conninfo: PostgreSQL connection string.
    """
    # Cache the batch: it's read twice below (valid and invalid rows).
    batch_df.persist()
    try:
        # Validation guaranteed trade_count is a whole number, so the cast
        # is safe. It matches the INTEGER column in PostgreSQL.
        valid = batch_df.filter("rejection_reason IS NULL").withColumn(
            "trade_count", F.col("trade_count").cast("long")
        )
        _insert(
            conninfo,
            "raw_prices",
            RAW_PRICE_COLUMNS,
            _collect_rows(valid, RAW_PRICE_COLUMNS),
        )

        invalid = batch_df.filter("rejection_reason IS NOT NULL")
        _insert(
            conninfo,
            "rejected_records",
            ["raw_payload", "reason"],  # Column names in the PostgreSQL table
            _collect_rows(invalid, ["raw_payload", "rejection_reason"]),  # In Spark
        )
    finally:
        batch_df.unpersist()


def write_metrics_batch(batch_df: DataFrame, conninfo: str) -> None:
    """Write finalised 5-minute window metrics to stock_metrics.

    Args:
        batch_df: One micro-batch of window aggregates.
        conninfo: PostgreSQL connection string.
    """
    _insert(
        conninfo,
        "stock_metrics",
        METRIC_COLUMNS,
        _collect_rows(batch_df, METRIC_COLUMNS),
    )
