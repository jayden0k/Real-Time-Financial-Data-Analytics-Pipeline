"""
Processing service entrypoint.

Pipeline per message:
    Kafka record
      -> normalize_kafka_message()      (strict re-validation, UTC)
      -> SlidingWindowRegistry.add()    (thread-safe rolling VWAP/mean/stdev)
      -> AnomalyDetector.check_tick()   (pure z-score rule + structured WARN log)
      -> ProcessedTick                  (schema combining tick + stats + flag)
      -> TimeSeriesWriter.enqueue()     (batched, non-blocking persistence)

Offsets are committed only *after* a message has been fully processed and
handed to the storage writer's queue, so a crash mid-batch results in
at-least-once reprocessing rather than silent data loss.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from config.settings import get_settings
from common.logging_config import configure_logging
from processing.kafka_consumer import MarketDataConsumer
from processing.normalizer import normalize_kafka_message
from processing.sliding_window import SlidingWindowRegistry
from anomaly_detection.detector import AnomalyDetector
from schemas.market_data import ProcessedTick
from storage.db_writer import build_writer_from_settings

logger = logging.getLogger(__name__)


async def main() -> None:
    settings = get_settings()
    configure_logging("processing-service", settings.log_level)

    consumer = MarketDataConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_topic_raw,
        group_id=settings.kafka_consumer_group,
    )
    window_registry = SlidingWindowRegistry(window_seconds=settings.window_seconds)
    detector = AnomalyDetector(
        threshold=settings.anomaly_zscore_threshold,
        min_samples=settings.min_samples_for_anomaly_check,
    )
    writer = build_writer_from_settings(settings)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    await consumer.start()
    writer.start()

    try:
        async for record in consumer.messages():
            if stop_event.is_set():
                break

            tick = normalize_kafka_message(record.value)
            if tick is None:
                await consumer.commit()
                continue

            buffer = window_registry.get_buffer(tick.symbol)
            stats = buffer.add(tick.price, tick.quantity, tick.trade_time)

            anomaly_event = detector.check_tick(
                symbol=tick.symbol,
                price=tick.price,
                timestamp=tick.trade_time,
                stats=stats,
            )

            processed = ProcessedTick(
                symbol=tick.symbol,
                price=tick.price,
                quantity=tick.quantity,
                trade_time=tick.trade_time,
                side=tick.side,
                stats=stats,
                is_anomaly=anomaly_event is not None,
                anomaly_zscore=anomaly_event.deviation_zscore if anomaly_event else None,
            )
            writer.enqueue(processed)

            logger.debug(
                "Tick processed",
                extra={
                    "extra_fields": {
                        "symbol": processed.symbol,
                        "vwap": round(stats.vwap, 4),
                        "is_anomaly": processed.is_anomaly,
                    }
                },
            )

            await consumer.commit()
    finally:
        writer.stop()
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
