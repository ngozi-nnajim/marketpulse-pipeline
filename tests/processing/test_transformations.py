"""Unit tests for Spark validation and window metrics.

These run inside the Spark image (Python 3.8 + Java), so they are skipped
automatically on machines without pyspark installed.
"""

from __future__ import annotations

import json
from datetime import datetime

import pytest

pytest.importorskip("pyspark")

from marketpulse.processing.transformations import (  # noqa: E402
    add_rejection_reason,
    compute_window_metrics,
    parse_messages,
)


def make_message(**overrides):
    """Build a valid message as JSON text, with optional field overrides."""
    message = {
        "schema_version": 1,
        "symbol": "AAPL",
        "asset_class": "stock",
        "bar_timestamp": "2026-10-19T14:30:00+00:00",
        "open": 100.0,
        "high": 101.0,
        "low": 99.5,
        "close": 100.5,
        "volume": 1200.0,
        "trade_count": 35,
        "vwap": 100.2,
        "ingested_at": "2026-10-19T14:31:00.123456+00:00",
    }
    message.update(overrides)
    return json.dumps(message)


def validated_df(spark, *payloads):
    """Run raw payloads through parsing and validation; return a DataFrame."""
    df = spark.createDataFrame([(p,) for p in payloads], ["value"])
    return add_rejection_reason(parse_messages(df))


def validate(spark, *payloads):
    """Run raw payloads through parsing and validation; return the rows."""
    return validated_df(spark, *payloads).collect()


def test_valid_bar_passes(spark):
    (row,) = validate(spark, make_message())
    assert row.rejection_reason is None


def test_timestamps_with_microseconds_and_offsets_parse(spark):
    (row,) = validate(spark, make_message())
    assert row.bar_timestamp is not None
    assert row.ingested_at is not None


def test_whole_number_float_trade_count_is_accepted(spark):
    # Regression test: 35.0 used to be parsed as null and rejected.
    (row,) = validate(spark, make_message(trade_count=35.0))
    assert row.rejection_reason is None


def test_zero_volume_crypto_bar_is_valid(spark):
    # Thin crypto markets have minutes with prices but no trades.
    payload = make_message(
        symbol="BTC/USD", asset_class="crypto", volume=0.0, trade_count=0
    )
    (row,) = validate(spark, payload)
    assert row.rejection_reason is None


def test_malformed_json_is_rejected(spark):
    (row,) = validate(spark, "this is not json")
    assert row.rejection_reason == "malformed_or_missing_symbol"


@pytest.mark.parametrize(
    ("overrides", "expected_reason"),
    [
        ({"schema_version": 2}, "unsupported_schema_version"),
        ({"asset_class": "bond"}, "invalid_asset_class"),
        ({"bar_timestamp": "not-a-date"}, "invalid_bar_timestamp"),
        ({"close": 0}, "missing_or_non_positive_price"),
        ({"high": 98.0}, "high_below_low"),
        ({"open": 105.0}, "open_or_close_outside_high_low"),
        ({"volume": -1}, "invalid_volume"),
        ({"trade_count": 1.5}, "invalid_trade_count"),
    ],
)
def test_invalid_bars_are_rejected_with_reason(spark, overrides, expected_reason):
    (row,) = validate(spark, make_message(**overrides))
    assert row.rejection_reason == expected_reason


def test_window_metrics_aggregate_bars_in_same_window(spark):
    # Stay in Spark end to end, as the real job does. Rebuilding a DataFrame
    # from collected rows fails when a column holds only nulls, because
    # Spark can't infer a type from nothing but None.
    valid = validated_df(
        spark,
        make_message(bar_timestamp="2026-10-19T14:30:00+00:00", close=100.0),
        make_message(bar_timestamp="2026-10-19T14:31:00+00:00", close=101.0),
    ).filter("rejection_reason IS NULL")
    (metric,) = compute_window_metrics(valid).collect()
    assert metric.window_start == datetime(2026, 10, 19, 14, 30)
    assert metric.avg_close == pytest.approx(100.5)
    assert metric.bar_count == 2
