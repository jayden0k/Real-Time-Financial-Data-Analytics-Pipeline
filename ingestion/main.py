"""
Ingestion service entrypoint.

Producer-Consumer pattern, half 1: this process is the *producer*. It has
exactly one job -- get ticks off the exchange WebSocket and onto Kafka as
fast and reliably as possible. It knows nothing about VWAP, anomaly
detection, or storage. That separation is what lets ingestion and
processing scale, deploy, and fail independently.
"""

from __future__ import annotations

import asyncio
import logging

from config.settings import get_settings
from common.logging_config import configure_logging
from ingestion.kafka_producer import MarketDataProducer
from ingestion.websocket_client import ResilientWebSocketClient
from schemas.market_data import RawTick

logger = logging.getLogger(__name__)


async def main() -> None:
    settings = get_settings()
    configure_logging("ingestion-service", settings.log_level)

    producer = MarketDataProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_topic_raw,
    )
    await producer.start()

    async def on_tick(tick: RawTick) -> None:
        await producer.send_tick(tick)
        logger.debug(
            "Tick published",
            extra={"extra_fields": {"symbol": tick.symbol, "price": tick.price}},
        )

    client = ResilientWebSocketClient(url=settings.ws_url, on_tick=on_tick)
    client.install_signal_handlers()

    try:
        await client.run()
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
