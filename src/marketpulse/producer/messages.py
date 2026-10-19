"""Turn Alpaca bars into Kafka message payloads.

Kept separate from the streaming code so it can be unit-tested without any
network connections.
"""

from datetime import datetime
from typing import Any, Literal

from alpaca.data.models import Bar

# Bump this whenever the message format changes, so consumers can tell
# old and new messages apart.
SCHEMA_VERSION = 1

AssetClass = Literal["stock", "crypto"]


def build_message(
    bar: Bar, asset_class: AssetClass, ingested_at: datetime
) -> dict[str, Any]:
    """Convert one Alpaca bar into a JSON-serialisable dictionary.

    Args:
        bar: A one-minute bar received from Alpaca's WebSocket.
        asset_class: Whether the bar is for a stock or a cryptocurrency.
        ingested_at: When the producer received the bar, in UTC.

    Returns:
        A flat dictionary following message schema version 1. Timestamps are
        ISO-8601 strings, because JSON has no native date type.
    """
    return {
        "schema_version": SCHEMA_VERSION,
        "symbol": bar.symbol,
        "asset_class": asset_class,
        "bar_timestamp": bar.timestamp.isoformat(),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        # The SDK returns trade_count as a float (e.g. 184.0), but the message
        # contract defines it as an integer: you can't have half a trade.
        "trade_count": int(bar.trade_count) if bar.trade_count is not None else None,
        "vwap": bar.vwap,
        "ingested_at": ingested_at.isoformat(),
    }
