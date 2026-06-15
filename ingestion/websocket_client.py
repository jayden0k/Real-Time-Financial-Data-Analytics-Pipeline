"""
Resilient asyncio WebSocket client for a live market-data feed.

Design notes
------------
- Exponential backoff with jitter and a hard cap prevents hammering the
  exchange's endpoint during an outage (and avoids IP bans).
- A `asyncio.Event` (`_shutdown`) is the cooperative-cancellation signal;
  SIGINT/SIGTERM handlers set it, and every loop iteration checks it, so
  shutdown is graceful rather than an abrupt `sys.exit()`.
- Malformed/partial messages are logged and skipped rather than crashing
  the connection -- a single bad tick must never take down ingestion.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import signal
from typing import Awaitable, Callable

import websockets
from websockets.exceptions import ConnectionClosed
from pydantic import ValidationError

from schemas.market_data import RawTick

logger = logging.getLogger(__name__)

TickHandler = Callable[[RawTick], Awaitable[None]]


class ResilientWebSocketClient:
    def __init__(
        self,
        url: str,
        on_tick: TickHandler,
        max_backoff_seconds: float = 60.0,
        base_backoff_seconds: float = 1.0,
    ) -> None:
        self._url = url
        self._on_tick = on_tick
        self._max_backoff = max_backoff_seconds
        self._base_backoff = base_backoff_seconds
        self._shutdown = asyncio.Event()

    def install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._trigger_shutdown)
            except NotImplementedError:
                # add_signal_handler isn't available on Windows event loops.
                signal.signal(sig, lambda *_: self._trigger_shutdown())

    def _trigger_shutdown(self) -> None:
        logger.info("Shutdown signal received, closing WebSocket gracefully...")
        self._shutdown.set()

    async def run(self) -> None:
        attempt = 0
        while not self._shutdown.is_set():
            try:
                logger.info("Connecting to WebSocket", extra={"extra_fields": {"url": self._url}})
                async with websockets.connect(self._url, ping_interval=20, ping_timeout=20) as ws:
                    attempt = 0  # reset backoff after a successful connection
                    await self._consume(ws)
            except (ConnectionClosed, OSError, asyncio.TimeoutError) as exc:
                if self._shutdown.is_set():
                    break
                attempt += 1
                delay = self._compute_backoff(attempt)
                logger.warning(
                    "WebSocket connection lost, retrying with backoff",
                    extra={
                        "extra_fields": {
                            "attempt": attempt,
                            "delay_seconds": round(delay, 2),
                            "error": str(exc),
                        }
                    },
                )
                await self._sleep_or_shutdown(delay)

        logger.info("WebSocket client stopped cleanly")

    async def _consume(self, ws: websockets.WebSocketClientProtocol) -> None:
        async for raw_message in ws:
            if self._shutdown.is_set():
                break
            await self._handle_message(raw_message)

    async def _handle_message(self, raw_message: str | bytes) -> None:
        try:
            payload = json.loads(raw_message)
            tick = RawTick.from_binance_payload(payload)
        except (json.JSONDecodeError, KeyError, ValidationError, ValueError) as exc:
            logger.warning(
                "Dropping malformed tick", extra={"extra_fields": {"error": str(exc)}}
            )
            return

        await self._on_tick(tick)

    def _compute_backoff(self, attempt: int) -> float:
        exp = min(self._base_backoff * (2 ** attempt), self._max_backoff)
        jitter = random.uniform(0, exp * 0.25)
        return exp + jitter

    async def _sleep_or_shutdown(self, delay: float) -> None:
        try:
            await asyncio.wait_for(self._shutdown.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass  # normal path: delay elapsed, retry connecting
