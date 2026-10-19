"""Pure DataFrame transformations for the streaming job.

Each function takes a DataFrame and returns a new one, with no I/O, so the
logic can be unit-tested without Kafka or PostgreSQL.
"""

# Spark's image runs Python 3.8. This makes Python treat type hints as plain
# text instead of evaluating them, so modern hints like list[str] and
# `X | None` don't crash on older versions.
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

SUPPORTED_SCHEMA_VERSION = 1
VALID_ASSET_CLASSES = ["stock", "crypto"]

# The expected shape of each Kafka message (the producer's contract).
# Timestamps are read as strings and converted afterwards: a cast handles
# ISO-8601 with microseconds and UTC offsets more reliably than JSON parsing.
MESSAGE_SCHEMA = StructType(
    [
        StructField("schema_version", IntegerType()),
        StructField("symbol", StringType()),
        StructField("asset_class", StringType()),
        StructField("bar_timestamp", StringType()),
        StructField("open", DoubleType()),
        StructField("high", DoubleType()),
        StructField("low", DoubleType()),
        StructField("close", DoubleType()),
        StructField("volume", DoubleType()),
        # Read as a double so both 184 and 184.0 parse. The validation below
        # still rejects true fractions like 184.5.
        StructField("trade_count", DoubleType()),
        StructField("vwap", DoubleType()),
        StructField("ingested_at", StringType()),
    ]
)


def parse_messages(kafka_df: DataFrame) -> DataFrame:
    """Turn raw Kafka records into typed columns.

    Args:
        kafka_df: DataFrame from Spark's Kafka source (binary `value` column).

    Returns:
        One column per message field, plus `raw_payload` holding the original
        JSON text so rejected records can be stored exactly as received.
    """
    return (
        kafka_df.select(F.col("value").cast("string").alias("raw_payload"))
        .withColumn("data", F.from_json("raw_payload", MESSAGE_SCHEMA))
        .select("raw_payload", "data.*")
        .withColumn("bar_timestamp", F.col("bar_timestamp").cast("timestamp"))
        .withColumn("ingested_at", F.col("ingested_at").cast("timestamp"))
    )


def _any_null_or_non_positive(*names: str) -> Column:
    """Build a condition that is true if any named price column is invalid."""
    condition = F.lit(False)
    for name in names:
        condition = condition | F.col(name).isNull() | (F.col(name) <= 0)
    return condition


def add_rejection_reason(parsed_df: DataFrame) -> DataFrame:
    """Add a `rejection_reason` column: null for valid rows, else why it failed.

    Rules are checked in order and the first failure wins. Zero volume and
    zero trade_count are deliberately VALID: thin crypto markets often have
    minutes with prices but no trades (observed during exploration).

    Args:
        parsed_df: Output of `parse_messages`.

    Returns:
        The same rows with an extra `rejection_reason` column.
    """
    prices = ("open", "high", "low", "close")
    reason = (
        F.when(F.col("symbol").isNull(), "malformed_or_missing_symbol")
        .when(
            F.col("schema_version") != SUPPORTED_SCHEMA_VERSION,
            "unsupported_schema_version",
        )
        .when(~F.col("asset_class").isin(VALID_ASSET_CLASSES), "invalid_asset_class")
        .when(F.col("bar_timestamp").isNull(), "invalid_bar_timestamp")
        .when(_any_null_or_non_positive(*prices), "missing_or_non_positive_price")
        .when(F.col("high") < F.col("low"), "high_below_low")
        .when(
            (F.col("open") > F.col("high"))
            | (F.col("open") < F.col("low"))
            | (F.col("close") > F.col("high"))
            | (F.col("close") < F.col("low")),
            "open_or_close_outside_high_low",
        )
        .when(F.col("volume").isNull() | (F.col("volume") < 0), "invalid_volume")
        .when(
            F.col("trade_count").isNull()
            | (F.col("trade_count") < 0)
            | (F.col("trade_count") != F.floor("trade_count")),
            "invalid_trade_count",
        )
    )
    return parsed_df.withColumn("rejection_reason", reason)


def compute_window_metrics(valid_df: DataFrame) -> DataFrame:
    """Aggregate valid bars into 5-minute windows per symbol.

    The watermark tells Spark to wait for bars up to 2 minutes late, then
    finalise the window. Without it, Spark would keep every window open
    forever, and memory would grow without limit.

    Args:
        valid_df: Rows that passed validation.

    Returns:
        One row per symbol per 5-minute window.
    """
    return (
        valid_df.withWatermark("bar_timestamp", "2 minutes")
        .groupBy(F.window("bar_timestamp", "5 minutes"), F.col("symbol"))
        .agg(
            F.avg("close").alias("avg_close"),
            F.min("low").alias("min_low"),
            F.max("high").alias("max_high"),
            F.sum("volume").alias("total_volume"),
            F.count("*").alias("bar_count"),
        )
        .select(
            "symbol",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "avg_close",
            "min_low",
            "max_high",
            "total_volume",
            "bar_count",
        )
    )
