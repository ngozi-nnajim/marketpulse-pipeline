"""Application settings loaded from environment variables.

pydantic-settings reads each field from an environment variable with the same
name (case-insensitive), falling back to a local .env file, and checks the
types. A missing or invalid value fails at startup with a clear message,
instead of causing a confusing error deep inside the pipeline.
"""

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Typed configuration for the MarketPulse pipeline."""

    # extra="ignore": .env also holds Postgres settings this class doesn't use.
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # SecretStr hides the value when printed or logged ("**********"),
    # so keys can't leak into log files by accident.
    alpaca_api_key: SecretStr
    alpaca_secret_key: SecretStr

    # Default works from the local system; docker-compose overrides it for containers.
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "stock_prices_raw"

    # Lists are given as JSON in env vars, e.g. STOCK_SYMBOLS='["AAPL","MSFT"]'.
    # The free plan allows up to 30 stock symbols on the WebSocket.
    stock_symbols: list[str] = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"]
    crypto_symbols: list[str] = ["BTC/USD", "ETH/USD", "SOL/USD"]
    stream_stocks: bool = True
    stream_crypto: bool = True


def get_settings() -> Settings:
    """Load and validate settings from the environment.

    Returns:
        A validated Settings instance.
    """
    return Settings()
