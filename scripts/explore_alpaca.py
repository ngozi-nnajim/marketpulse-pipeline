"""Exploration script: fetch recent market data from Alpaca.

A throwaway "spike" to learn the shape of Alpaca's data before building
the real producer. Run this from the project root with:

    uv run --env-file .env python scripts/explore_alpaca.py
"""

import os
from datetime import UTC, datetime, timedelta

import pandas as pd
from alpaca.data.enums import DataFeed
from alpaca.data.historical import CryptoHistoricalDataClient, StockHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame

pd.set_option("display.max_columns", None)  # show all columns


def fetch_crypto_bars() -> None:
    """Fetch and print the last hour of 1-minute Bitcoin bars.

    Crypto historical data needs no API keys, so this confirms the SDK and
    network work before we involve credentials.
    """
    client = CryptoHistoricalDataClient()
    request = CryptoBarsRequest(
        symbol_or_symbols=["BTC/USD"],
        timeframe=TimeFrame.Minute,
        # Always use timezone-aware UTC times in data pipelines. "Naive"
        # times without a timezone are a classic source of off-by-hours bugs.
        start=datetime.now(UTC) - timedelta(hours=1),
    )
    bars = client.get_crypto_bars(request)
    print("=== BTC/USD: last 5 one-minute bars ===")
    print(bars.df.tail())


def fetch_stock_bars() -> None:
    """Fetch and print recent 1-minute bars for two stocks.

    Uses the IEX feed, which is the real-time feed included in Alpaca's
    free plan. The full-market (SIP) feed requires a paid subscription.
    """
    client = StockHistoricalDataClient(
        # os.environ["..."] (rather than os.getenv) crashes immediately with a
        # clear KeyError if a key is missing. Failing fast beats failing later
        # with a confusing authentication error.
        api_key=os.environ["ALPACA_API_KEY"],
        secret_key=os.environ["ALPACA_SECRET_KEY"],
    )
    request = StockBarsRequest(
        symbol_or_symbols=["AAPL", "MSFT"],
        timeframe=TimeFrame.Minute,
        # Look back 5 days so we get data even on weekends and holidays,
        # when the US stock market is closed.
        start=datetime.now(UTC) - timedelta(days=5),
        feed=DataFeed.IEX,
    )
    bars = client.get_stock_bars(request)
    print("\n=== AAPL and MSFT: last 10 one-minute bars ===")
    print(bars.df.tail(10))


if __name__ == "__main__":
    fetch_crypto_bars()
    fetch_stock_bars()
