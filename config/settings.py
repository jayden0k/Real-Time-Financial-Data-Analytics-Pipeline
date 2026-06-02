"""
Centralized, validated configuration.

Design pattern: Singleton (via functools.lru_cache).
--------------------------------------------------------------------
`get_settings()` is the *only* sanctioned way to read configuration
anywhere in the codebase. Because it's wrapped in `@lru_cache`, the
Settings object is constructed and validated exactly once per process,
then reused. This guarantees:
  1. Env vars are parsed/validated a single time (fail fast on boot).
  2. No two modules can ever see a different config snapshot.
  3. Tests can bypass the singleton by calling `Settings(**overrides)`
     directly, or by clearing the cache with `get_settings.cache_clear()`.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- Market data source ---
    ws_url: str = Field(default="wss://stream.binance.com:9443/ws/btcusdt@trade")
    symbols: str = Field(default="BTCUSDT")

    # --- Kafka ---
    kafka_bootstrap_servers: str = Field(default="localhost:9092")
    kafka_topic_raw: str = Field(default="market-data-raw")
    kafka_consumer_group: str = Field(default="processing-service")

    # --- Sliding window / anomaly detection ---
    window_seconds: float = Field(default=10.0, gt=0)
    anomaly_zscore_threshold: float = Field(default=3.0, gt=0)
    min_samples_for_anomaly_check: int = Field(default=5, ge=2)

    # --- Storage ---
    storage_backend: str = Field(default="sqlite")
    sqlite_db_path: str = Field(default="./data/market_data.db")
    storage_batch_size: int = Field(default=50, gt=0)
    storage_flush_interval_seconds: float = Field(default=2.0, gt=0)

    # --- TimescaleDB ---
    timescale_dsn: str = Field(
        default="postgresql://postgres:postgres@localhost:5432/market_data"
    )

    # --- Logging ---
    log_level: str = Field(default="INFO")
    environment: str = Field(default="development")

    @field_validator("storage_backend")
    @classmethod
    def _validate_backend(cls, v: str) -> str:
        allowed = {"sqlite", "timescale"}
        if v.lower() not in allowed:
            raise ValueError(f"storage_backend must be one of {allowed}, got {v!r}")
        return v.lower()

    @property
    def symbol_list(self) -> list[str]:
        return [s.strip().upper() for s in self.symbols.split(",") if s.strip()]

    def ensure_sqlite_dir(self) -> None:
        Path(self.sqlite_db_path).parent.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide singleton accessor. Cached after first call."""
    return Settings()
