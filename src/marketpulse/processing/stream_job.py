"""Spark Structured Streaming job: Kafka -> validate -> PostgreSQL.

Runs two streaming queries over the same Kafka topic:
  1. raw:     every valid bar -> raw_prices; invalid -> rejected_records
  2. metrics: 5-minute window aggregates -> stock_metrics
Each query keeps its own checkpoint, so both resume where they stopped.
"""

# Spark's image runs Python 3.8. This makes Python treat type hints as plain
# text instead of evaluating them, so modern hints like list[str] and
# `X | None` don't crash on older versions.
from __future__ import annotations

import logging
import os

from pyspark.sql import DataFrame, SparkSession

from marketpulse.processing.sink import write_metrics_batch, write_raw_batch
from marketpulse.processing.transformations import (
    add_rejection_reason,
    compute_window_metrics,
    parse_messages,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def build_conninfo() -> str:
    """Build the PostgreSQL connection string from environment variables.

    Returns:
        A libpq connection string. The password is never logged.
    """
    return (
        f"host={os.environ['POSTGRES_HOST']} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"dbname={os.environ['POSTGRES_DB']} "
        f"user={os.environ['POSTGRES_USER']} "
        f"password={os.environ['POSTGRES_PASSWORD']}"
    )


def build_spark() -> SparkSession:
    """Create the Spark session with settings suited to this small stream."""
    return (
        SparkSession.builder.appName("marketpulse-stream")
        # Interpret all timestamps in UTC, regardless of machine settings.
        .config("spark.sql.session.timeZone", "UTC")
        # The default (200) shuffle partitions is overkill for tiny batches
        # and would make each micro-batch needlessly slow.
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )


def read_kafka(spark: SparkSession, servers: str, topic: str) -> DataFrame:
    """Open a streaming read on the Kafka topic.

    Args:
        spark: Active Spark session.
        servers: Kafka bootstrap servers.
        topic: Topic to subscribe to.

    Returns:
        A streaming DataFrame of raw Kafka records.
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", servers)
        .option("subscribe", topic)
        # First run only: process everything already in the topic.
        # Later runs resume from the checkpoint instead.
        .option("startingOffsets", "earliest")
        # Our dev Kafka has no persistent storage, so offsets can vanish when
        # it restarts. Don't crash over that locally (keep true in prod).
        .option("failOnDataLoss", "false")
        .load()
    )


def main() -> None:
    """Start both streaming queries and run until stopped."""
    conninfo = build_conninfo()
    checkpoint_dir = os.environ.get("CHECKPOINT_DIR", "/opt/checkpoints")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")  # Spark's INFO logs are very noisy

    validated = add_rejection_reason(
        parse_messages(
            read_kafka(
                spark,
                os.environ["KAFKA_BOOTSTRAP_SERVERS"],
                os.environ.get("KAFKA_TOPIC", "stock_prices_raw"),
            )
        )
    )

    raw_query = (
        validated.writeStream.queryName("raw_prices")
        .foreachBatch(lambda df, _batch_id: write_raw_batch(df, conninfo))
        .option("checkpointLocation", f"{checkpoint_dir}/raw")
        .trigger(processingTime="10 seconds")
        .start()
    )

    metrics = compute_window_metrics(validated.filter("rejection_reason IS NULL"))
    metrics_query = (
        metrics.writeStream.queryName("stock_metrics")
        # "append": emit each window once, after the watermark finalises it.
        .outputMode("append")
        .foreachBatch(lambda df, _batch_id: write_metrics_batch(df, conninfo))
        .option("checkpointLocation", f"{checkpoint_dir}/metrics")
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("Started queries: %s, %s", raw_query.name, metrics_query.name)
    # Block until either query fails; Docker then restarts the job.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
