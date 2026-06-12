"""
Thin async wrapper around aiokafka's AIOKafkaProducer.

Kept deliberately small: its only job is serialize-and-send. Keeping I/O
wrappers thin makes them trivial to mock in tests and easy to swap (e.g.
for confluent-kafka or an Avro-serializing producer) without touching
business logic elsewhere.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from aiokafka import AIOKafkaProducer

from schemas.market_data import RawTick

logger = logging.getLogger(__name__)


class MarketDataProducer:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self._topic = topic
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=self._serialize,
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",  # durability: wait for all in-sync replicas
            enable_idempotence=True,  # exactly-once producer semantics
            linger_ms=20,  # small batching window to reduce broker round trips
        )

    @staticmethod
    def _serialize(tick: dict[str, Any]) -> bytes:
        return json.dumps(tick, default=str).encode("utf-8")

    async def start(self) -> None:
        await self._producer.start()
        logger.info("Kafka producer started", extra={"extra_fields": {"topic": self._topic}})

    async def stop(self) -> None:
        await self._producer.stop()
        logger.info("Kafka producer stopped")

    async def send_tick(self, tick: RawTick) -> None:
        # Key by symbol so all ticks for a symbol land on the same partition,
        # preserving per-symbol ordering for downstream sliding-window logic.
        await self._producer.send_and_wait(
            self._topic,
            key=tick.symbol,
            value=tick.model_dump(mode="json"),
        )
