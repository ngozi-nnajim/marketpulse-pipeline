# ADR 0001: Use Alpaca as the market data source

**Status:** Accepted
**Date:** 2026-10-16

## Context
The project brief named Alpha Vantage's intraday stock API as the data source.
Its free tier allows 25 requests per day and 5 per minute, which cannot support
a real-time pipeline. It also offers no streaming connection, so we would have
to poll for updates.

## Options considered
1. **Alpha Vantage with a replay producer:** fetch history in a few calls, then
   replay it into Kafka to simulate a live stream.
2. **Alpaca Market Data API (free Basic plan):** real-time trades pushed over a
   WebSocket, up to 30 symbols, plus 200 historical API calls per minute.

## Decision
Use Alpaca (option 2), approved by the project assigner.

## Consequences
- **Positive:** genuinely real-time data; no replay workaround or local cache needed.
- **Negative:** the free plan covers only the IEX exchange, so volumes are a
  partial view of the market.
- **Negative:** the stock stream is quiet outside US market hours. Alpaca's 24/7 crypto
  stream will be used for testing evenings and weekends.
