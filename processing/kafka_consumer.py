"""
Thin async wrapper around aiokafka's AIOKafkaConsumer.

Producer-Consumer pattern, half 2: this is the *consumer* side of the Kafka
topic. It pulls at its own pace, independent of ingestion's publish rate --
if processing briefly falls behind, Kafka simply retains the backlog instead
of ticks being dropped or the ingestion service blocking.
"""

from __future__ import annotations

import logging
from typing import AsyncIterator

from aiokafka import AIOKafkaConsumer, ConsumerRecord

logger = logging.getLogger(__name__)


class MarketDataConsumer:
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str) -> None:
        self._consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,  # commit only after successful processing
            auto_offset_reset="latest",
        )

    async def start(self) -> None:
        await self._consumer.start()
        logger.info("Kafka consumer started")

    async def stop(self) -> None:
        await self._consumer.stop()
        logger.info("Kafka consumer stopped")

    async def messages(self) -> AsyncIterator[ConsumerRecord]:
        async for record in self._consumer:
            yield record

    async def commit(self) -> None:
        await self._consumer.commit()
