"""Unit tests for the producer's Kafka message builder."""

import json
from datetime import UTC, datetime
from types import SimpleNamespace

from marketpulse.producer.messages import SCHEMA_VERSION, build_message


def make_bar(**overrides):
    """Create a fake Alpaca bar. SimpleNamespace mimics the SDK's Bar object."""
    fields = {
        "symbol": "AAPL",
        "timestamp": datetime(2026, 10, 19, 14, 30, tzinfo=UTC),
        "open": 100.0,
        "high": 101.0,
        "low": 99.5,
        "close": 100.5,
        "volume": 1200.0,
        "trade_count": 35.0,  # The SDK really returns a float here
        "vwap": 100.2,
    }
    fields.update(overrides)
    return SimpleNamespace(**fields)


INGESTED_AT = datetime(2026, 10, 19, 14, 31, tzinfo=UTC)


def test_trade_count_is_converted_to_int():
    # Regression test: a float trade_count once caused every bar to be rejected.
    message = build_message(make_bar(), "stock", INGESTED_AT)
    assert message["trade_count"] == 35
    assert isinstance(message["trade_count"], int)


def test_missing_trade_count_stays_none():
    message = build_message(make_bar(trade_count=None), "stock", INGESTED_AT)
    assert message["trade_count"] is None


def test_timestamps_are_iso_8601_strings():
    message = build_message(make_bar(), "stock", INGESTED_AT)
    assert message["bar_timestamp"] == "2026-10-19T14:30:00+00:00"
    assert message["ingested_at"] == "2026-10-19T14:31:00+00:00"


def test_contract_metadata_is_included():
    message = build_message(make_bar(), "crypto", INGESTED_AT)
    assert message["schema_version"] == SCHEMA_VERSION
    assert message["asset_class"] == "crypto"


def test_message_is_json_serialisable():
    message = build_message(make_bar(), "stock", INGESTED_AT)
    assert json.loads(json.dumps(message)) == message
