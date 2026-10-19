"""Producer: stream one-minute bars from Alpaca into Kafka.

Opens WebSocket connections to Alpaca (stocks and/or crypto), converts each
bar into a JSON message, and publishes it to Kafka keyed by symbol, so each
symbol's bars stay in order within one partition.

Run locally:    uv run python -m marketpulse.producer.main
Run in Docker:  docker compose up -d producer
"""

import json
import logging
import signal
import sys
import threading
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from types import FrameType

from alpaca.data.enums import DataFeed
from alpaca.data.live import CryptoDataStream, StockDataStream
from alpaca.data.models import Bar
from confluent_kafka import KafkaError, Message, Producer

from marketpulse.config import get_settings
from marketpulse.producer.messages import AssetClass, build_message

# Logging instead of print(): every line gets a timestamp and severity level,
# and output can be filtered or shipped to monitoring tools later.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def create_kafka_producer(bootstrap_servers: str) -> Producer:
    """Create a Kafka producer configured for reliable delivery.

    Args:
        bootstrap_servers: Kafka address, e.g. "kafka:19092".

    Returns:
        A configured confluent-kafka Producer.
    """
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "marketpulse-producer",
            # Only treat a message as sent once Kafka confirms it's stored.
            "acks": "all",
            # If a send is retried, Kafka still stores the message only once.
            "enable.idempotence": True,
            # Wait up to 50 ms to batch messages into fewer network requests.
            "linger.ms": 50,
        }
    )


def on_delivery(err: KafkaError | None, msg: Message) -> None:
    """Log the result of each message delivery.

    Kafka sends messages in the background, so success or failure is only
    known later, when the client calls this function.
    """
    if err is not None:
        logger.error("Delivery failed for key=%s: %s", msg.key(), err)


def make_bar_handler(
    producer: Producer, topic: str, asset_class: AssetClass
) -> Callable[[Bar], Awaitable[None]]:
    """Build the callback Alpaca calls for every incoming bar.

    Args:
        producer: Kafka producer used to publish messages.
        topic: Kafka topic to publish to.
        asset_class: "stock" or "crypto", recorded in each message.

    Returns:
        An async function that publishes one bar to Kafka.
    """

    async def handle_bar(bar: Bar) -> None:
        """Publish one bar to Kafka."""
        message = build_message(
            bar, asset_class=asset_class, ingested_at=datetime.now(UTC)
        )
        producer.produce(
            topic,
            key=bar.symbol,  # Same symbol -> same partition -> ordered bars
            value=json.dumps(message),
            on_delivery=on_delivery,
        )
        # produce() only queues the message; poll(0) lets the client run
        # any pending delivery callbacks without blocking.
        producer.poll(0)
        logger.info("Published %s %s", bar.symbol, message["bar_timestamp"])

    return handle_bar


def start_in_thread(target: Callable[[], None], name: str) -> threading.Thread:
    """Run a blocking function in a background thread.

    Each Alpaca stream's run() blocks forever, so the stock and crypto
    streams each get their own thread and can run at the same time.

    Args:
        target: The blocking function to run.
        name: Thread name, shown in log lines.

    Returns:
        The started thread.
    """
    # daemon=True: the thread won't keep the program alive at shutdown.
    thread = threading.Thread(target=target, name=name, daemon=True)
    thread.start()
    return thread


def main() -> None:
    """Start the configured streams and run until stopped."""
    settings = get_settings()
    producer = create_kafka_producer(settings.kafka_bootstrap_servers)
    api_key = settings.alpaca_api_key.get_secret_value()
    secret_key = settings.alpaca_secret_key.get_secret_value()

    threads: list[threading.Thread] = []

    if settings.stream_stocks:
        # IEX is the real-time stock feed included in the free plan.
        stock_stream = StockDataStream(api_key, secret_key, feed=DataFeed.IEX)
        stock_stream.subscribe_bars(
            make_bar_handler(producer, settings.kafka_topic, "stock"),
            *settings.stock_symbols,
        )
        threads.append(start_in_thread(stock_stream.run, "stock-stream"))
        logger.info("Subscribed to stock bars: %s", settings.stock_symbols)

    if settings.stream_crypto:
        crypto_stream = CryptoDataStream(api_key, secret_key)
        crypto_stream.subscribe_bars(
            make_bar_handler(producer, settings.kafka_topic, "crypto"),
            *settings.crypto_symbols,
        )
        threads.append(start_in_thread(crypto_stream.run, "crypto-stream"))
        logger.info("Subscribed to crypto bars: %s", settings.crypto_symbols)

    if not threads:
        logger.error("Both streams are disabled; nothing to do.")
        sys.exit(1)

    stop_event = threading.Event()

    def request_stop(signum: int, frame: FrameType | None) -> None:
        """Handle Ctrl+C (SIGINT) and `docker stop` (SIGTERM)."""
        logger.info("Received signal %s, shutting down.", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    # Main thread acts as a supervisor: if a stream dies, exit with an error
    # so Docker's restart policy starts a fresh producer. Crashing loudly and
    # restarting is more reliable than limping on without data.
    exit_code = 0
    while not stop_event.wait(timeout=1):
        dead = [t.name for t in threads if not t.is_alive()]
        if dead:
            logger.error("Stream thread(s) stopped unexpectedly: %s", dead)
            exit_code = 1
            break

    # Deliver any messages still queued in memory before exiting.
    undelivered = producer.flush(10)
    if undelivered:
        logger.warning("%d message(s) were not delivered.", undelivered)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
