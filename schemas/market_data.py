"""
Schema contracts for every hop of the pipeline.

Why Pydantic instead of raw Avro binary here: Pydantic gives us strict runtime
type validation, coercion, and JSON (de)serialization with zero extra infra,
which is ideal for a portfolio-grade pipeline. In a real enterprise deployment
these same field definitions would be mirrored 1:1 into an Avro (.avsc) schema
registered in Confluent Schema Registry -- the class docstrings below note the
equivalent Avro type for that migration path.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, Field, field_validator


class TradeSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class RawTick(BaseModel):
    """Validated shape of a single trade tick as it enters the system.

    Avro equivalent (market-data-raw.avsc):
        {"name": "symbol", "type": "string"}
        {"name": "price", "type": "double"}
        {"name": "quantity", "type": "double"}
        {"name": "trade_time", "type": {"type": "long", "logicalType": "timestamp-millis"}}
        {"name": "side", "type": "string"}
        {"name": "source", "type": "string"}
    """

    symbol: str = Field(..., min_length=1)
    price: float = Field(..., gt=0)
    quantity: float = Field(..., gt=0)
    trade_time: datetime  # normalized to UTC by validator below
    side: TradeSide
    source: str = Field(default="binance")

    @field_validator("symbol")
    @classmethod
    def _upper_symbol(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("trade_time")
    @classmethod
    def _force_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)

    @classmethod
    def from_binance_payload(cls, payload: dict) -> "RawTick":
        """Strict parser for Binance's raw @trade WebSocket payload.

        Binance trade payload fields we care about:
          s = symbol, p = price, q = quantity, T = trade time (ms epoch), m = is_buyer_maker
        """
        return cls(
            symbol=payload["s"],
            price=float(payload["p"]),
            quantity=float(payload["q"]),
            trade_time=datetime.fromtimestamp(payload["T"] / 1000.0, tz=timezone.utc),
            side=TradeSide.SELL if payload.get("m") else TradeSide.BUY,
            source="binance",
        )


class WindowStats(BaseModel):
    """Rolling-window statistics attached to each processed tick."""

    vwap: float
    mean_price: float
    stdev_price: float
    sample_count: int
    window_seconds: float


class ProcessedTick(BaseModel):
    """Output of the processing service: raw tick + rolling stats + anomaly flag."""

    symbol: str
    price: float
    quantity: float
    trade_time: datetime
    side: TradeSide
    stats: WindowStats
    is_anomaly: bool = False
    anomaly_zscore: float | None = None


class AnomalyEvent(BaseModel):
    """Emitted (and logged) when a tick is flagged as an anomaly."""

    symbol: str
    timestamp: datetime
    triggering_price: float
    rolling_mean: float
    rolling_stdev: float
    deviation_zscore: float
    threshold: float
