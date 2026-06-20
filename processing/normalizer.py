"""
Strict parsing/normalization layer between Kafka and business logic.

The ingestion service already validated ticks once via Pydantic before
publishing, but the processing service treats Kafka as an untrusted boundary
too (schema evolution, replays of old messages, another producer entirely)
and re-validates on the way in. Cheap insurance against "trust the pipe".
"""

from __future__ import annotations

import json
import logging

from pydantic import ValidationError

from schemas.market_data import RawTick

logger = logging.getLogger(__name__)


def normalize_kafka_message(raw_value: bytes) -> RawTick | None:
    """Parse a raw Kafka message value into a validated, UTC-normalized RawTick.

    Returns None (and logs a warning) for any malformed message rather than
    raising, so a single bad record never stalls the consumer loop.
    """
    try:
        payload = json.loads(raw_value.decode("utf-8"))
        return RawTick.model_validate(payload)
    except (json.JSONDecodeError, ValidationError, UnicodeDecodeError) as exc:
        logger.warning(
            "Dropping malformed Kafka message",
            extra={"extra_fields": {"error": str(exc)}},
        )
        return None
