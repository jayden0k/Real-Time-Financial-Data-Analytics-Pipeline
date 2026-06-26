"""
Thread-safe, time-based sliding-window buffer optimized for O(1) stats computation.

Uses running sum, sum of squares, and volume accumulators to evaluate rolling
VWAP, mean, and standard deviation without re-iterating the window deque on each tick.
"""

from __future__ import annotations

import math
import threading
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta

from schemas.market_data import WindowStats


@dataclass(frozen=True)
class PricePoint:
    price: float
    quantity: float
    timestamp: datetime


class SlidingWindowBuffer:
    """Rolling window of trades for a *single* symbol, keyed on wall-clock time."""

    def __init__(self, window_seconds: float) -> None:
        self._window = timedelta(seconds=window_seconds)
        self._window_seconds = window_seconds
        self._points: deque[PricePoint] = deque()
        self._lock = threading.Lock()

        # O(1) Running Accumulators
        self._sum_price: float = 0.0
        self._sum_price_sq: float = 0.0
        self._sum_pv: float = 0.0      # sum(price * quantity)
        self._sum_qty: float = 0.0     # sum(quantity)

    def add(self, price: float, quantity: float, timestamp: datetime) -> WindowStats:
        """Add a new point, evict stale points, and return fresh rolling stats in O(1)."""
        point = PricePoint(price, quantity, timestamp)
        pv = price * quantity
        price_sq = price * price

        with self._lock:
            # 1. Add point & update running accumulators
            self._points.append(point)
            self._sum_price += price
            self._sum_price_sq += price_sq
            self._sum_pv += pv
            self._sum_qty += quantity

            # 2. Evict stale points & deduct running accumulators
            self._evict_stale(reference_time=timestamp)

            # 3. Compute stats in O(1)
            return self._compute_stats()

    def snapshot(self) -> WindowStats:
        with self._lock:
            return self._compute_stats()

    def _evict_stale(self, reference_time: datetime) -> None:
        cutoff = reference_time - self._window
        while self._points and self._points[0].timestamp < cutoff:
            stale = self._points.popleft()
            # Deduct evicted values from accumulators
            self._sum_price -= stale.price
            self._sum_price_sq -= (stale.price * stale.price)
            self._sum_pv -= (stale.price * stale.quantity)
            self._sum_qty -= stale.quantity

    def _compute_stats(self) -> WindowStats:
        count = len(self._points)
        if count == 0:
            return WindowStats(
                vwap=0.0,
                mean_price=0.0,
                stdev_price=0.0,
                sample_count=0,
                window_seconds=self._window_seconds,
            )

        # O(1) Mean and VWAP calculation
        mean_price = self._sum_price / count
        vwap = (self._sum_pv / self._sum_qty) if self._sum_qty > 0 else mean_price

        # O(1) Population Standard Deviation calculation
        if count > 1:
            # Variance = E[X^2] - (E[X])^2
            variance = (self._sum_price_sq / count) - (mean_price * mean_price)
            # Clamp against floating point precision errors slightly below 0
            stdev_price = math.sqrt(max(0.0, variance))
        else:
            stdev_price = 0.0

        return WindowStats(
            vwap=vwap,
            mean_price=mean_price,
            stdev_price=stdev_price,
            sample_count=count,
            window_seconds=self._window_seconds,
        )


class SlidingWindowRegistry:
    """Owns one SlidingWindowBuffer per symbol. Lazily creates buffers on first use."""

    def __init__(self, window_seconds: float) -> None:
        self._window_seconds = window_seconds
        self._buffers: dict[str, SlidingWindowBuffer] = {}
        self._registry_lock = threading.Lock()

    def get_buffer(self, symbol: str) -> SlidingWindowBuffer:
        with self._registry_lock:
            if symbol not in self._buffers:
                self._buffers[symbol] = SlidingWindowBuffer(self._window_seconds)
            return self._buffers[symbol]