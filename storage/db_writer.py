"""
Batched time-series storage writer.

Design pattern: Strategy. `TimeSeriesWriter` is the abstract interface the
rest of the pipeline depends on; `SQLiteWriter` is the concrete, fully
working implementation shipped in this repo. `TimescaleWriter` is stubbed
to the same interface -- in a real deployment you'd implement its `_flush`
using `psycopg2.extras.execute_values` against the DSN in `config/settings`,
and nothing in `processing/main.py` would need to change.

Batching: writes are pushed onto an in-memory queue and flushed either when
`batch_size` rows have accumulated or `flush_interval_seconds` has elapsed
(whichever comes first) via a dedicated background thread. This is what
prevents per-tick I/O from becoming the pipeline's bottleneck under real
market load (hundreds of ticks/sec).
"""

from __future__ import annotations

import logging
import queue
import sqlite3
import threading
import time
from abc import ABC, abstractmethod
from datetime import datetime

from schemas.market_data import ProcessedTick
from storage.models import CREATE_INDEXES, CREATE_TICKS_TABLE, INSERT_TICK

logger = logging.getLogger(__name__)


class TimeSeriesWriter(ABC):
    """Abstract Strategy interface for time-series persistence."""

    @abstractmethod
    def start(self) -> None: ...

    @abstractmethod
    def stop(self) -> None: ...

    @abstractmethod
    def enqueue(self, tick: ProcessedTick) -> None: ...


class SQLiteWriter(TimeSeriesWriter):
    """Concrete Strategy: SQLite with a simulated day-partition key and batched writes.

    A single background thread owns the SQLite connection (SQLite connections
    are not safe to share across threads), draining a thread-safe `queue.Queue`
    that producers (the async processing loop) push onto via `enqueue()`.
    """

    def __init__(
        self,
        db_path: str,
        batch_size: int = 50,
        flush_interval_seconds: float = 2.0,
    ) -> None:
        self._db_path = db_path
        self._batch_size = batch_size
        self._flush_interval = flush_interval_seconds
        self._queue: queue.Queue[ProcessedTick | None] = queue.Queue()
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="sqlite-writer", daemon=True)
        self._thread.start()
        logger.info("SQLiteWriter started", extra={"extra_fields": {"db_path": self._db_path}})

    def stop(self) -> None:
        self._stop_event.set()
        self._queue.put(None)  # unblock a queue.get() if the worker is waiting
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("SQLiteWriter stopped")

    def enqueue(self, tick: ProcessedTick) -> None:
        self._queue.put(tick)

    # --- internal worker thread ---

    def _run(self) -> None:
        conn = sqlite3.connect(self._db_path, check_same_thread=True)
        conn.executescript(CREATE_TICKS_TABLE)
        conn.executescript(CREATE_INDEXES)
        conn.commit()

        buffer: list[ProcessedTick] = []
        last_flush = time.monotonic()

        while not self._stop_event.is_set() or not self._queue.empty():
            timeout = max(0.0, self._flush_interval - (time.monotonic() - last_flush))
            try:
                item = self._queue.get(timeout=timeout or 0.1)
            except queue.Empty:
                item = "TIMEOUT"  # sentinel meaning "just check flush conditions"

            if item not in (None, "TIMEOUT"):
                buffer.append(item)  # type: ignore[arg-type]

            should_flush = (
                len(buffer) >= self._batch_size
                or (time.monotonic() - last_flush) >= self._flush_interval
            )
            if should_flush and buffer:
                self._flush(conn, buffer)
                buffer = []
                last_flush = time.monotonic()

        if buffer:
            self._flush(conn, buffer)
        conn.close()

    def _flush(self, conn: sqlite3.Connection, batch: list[ProcessedTick]) -> None:
        rows = [self._to_row(tick) for tick in batch]
        try:
            conn.executemany(INSERT_TICK, rows)
            conn.commit()
            logger.info(
                "Flushed batch to storage",
                extra={"extra_fields": {"rows_written": len(rows)}},
            )
        except sqlite3.Error as exc:
            conn.rollback()
            logger.error(
                "Batch flush failed, rolling back",
                extra={"extra_fields": {"error": str(exc), "batch_size": len(rows)}},
            )

    @staticmethod
    def _to_row(tick: ProcessedTick) -> tuple:
        partition_key: str = tick.trade_time.strftime("%Y-%m-%d")
        return (
            tick.symbol,
            tick.price,
            tick.quantity,
            tick.trade_time.isoformat(),
            tick.side.value,
            tick.stats.vwap,
            tick.stats.mean_price,
            tick.stats.stdev_price,
            tick.stats.sample_count,
            int(tick.is_anomaly),
            tick.anomaly_zscore,
            partition_key,
        )


class TimescaleWriter(TimeSeriesWriter):
    """Stub Strategy for TimescaleDB, matching the same interface as SQLiteWriter.

    Swap-in steps for a real deployment:
      1. `CREATE TABLE processed_ticks (...); SELECT create_hypertable('processed_ticks', 'trade_time');`
      2. Implement `_flush` with `psycopg2.extras.execute_values(cur, INSERT_TICK_PG, rows)`.
      3. Point `config.settings.STORAGE_BACKEND=timescale` and wire this class
         in `processing/main.py` in place of SQLiteWriter -- no other code changes.
    """

    def __init__(self, dsn: str, batch_size: int = 50, flush_interval_seconds: float = 2.0) -> None:
        self._dsn = dsn
        self._batch_size = batch_size
        self._flush_interval = flush_interval_seconds

    def start(self) -> None:
        raise NotImplementedError(
            "TimescaleWriter is a Strategy stub. Implement _flush() with "
            "psycopg2 to enable it (see class docstring)."
        )

    def stop(self) -> None:
        pass

    def enqueue(self, tick: ProcessedTick) -> None:
        pass


def build_writer_from_settings(settings) -> TimeSeriesWriter:  # noqa: ANN001
    """Factory selecting the configured storage Strategy."""
    if settings.storage_backend == "timescale":
        return TimescaleWriter(
            dsn=settings.timescale_dsn,
            batch_size=settings.storage_batch_size,
            flush_interval_seconds=settings.storage_flush_interval_seconds,
        )
    settings.ensure_sqlite_dir()
    return SQLiteWriter(
        db_path=settings.sqlite_db_path,
        batch_size=settings.storage_batch_size,
        flush_interval_seconds=settings.storage_flush_interval_seconds,
    )
