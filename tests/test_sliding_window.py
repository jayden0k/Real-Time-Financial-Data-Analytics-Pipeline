from __future__ import annotations

import threading
from datetime import timedelta

from processing.sliding_window import SlidingWindowBuffer, SlidingWindowRegistry


def test_vwap_computation_matches_manual_calculation(base_time):
    buf = SlidingWindowBuffer(window_seconds=10)
    buf.add(price=100.0, quantity=1.0, timestamp=base_time)
    stats = buf.add(price=200.0, quantity=3.0, timestamp=base_time)

    # VWAP = (100*1 + 200*3) / (1+3) = 700/4 = 175
    assert stats.vwap == 175.0
    assert stats.sample_count == 2


def test_stale_points_are_evicted_outside_window(base_time):
    buf = SlidingWindowBuffer(window_seconds=10)
    buf.add(price=100.0, quantity=1.0, timestamp=base_time)

    # 11 seconds later -> the first point should be evicted
    later = base_time + timedelta(seconds=11)
    stats = buf.add(price=150.0, quantity=1.0, timestamp=later)

    assert stats.sample_count == 1
    assert stats.mean_price == 150.0


def test_empty_buffer_returns_zeroed_stats():
    buf = SlidingWindowBuffer(window_seconds=10)
    stats = buf.snapshot()

    assert stats.sample_count == 0
    assert stats.vwap == 0.0
    assert stats.stdev_price == 0.0


def test_registry_creates_isolated_buffers_per_symbol(base_time):
    registry = SlidingWindowRegistry(window_seconds=10)
    btc_buffer = registry.get_buffer("BTCUSDT")
    eth_buffer = registry.get_buffer("ETHUSDT")

    btc_buffer.add(price=60000.0, quantity=1.0, timestamp=base_time)
    eth_stats = eth_buffer.snapshot()

    assert eth_stats.sample_count == 0
    assert registry.get_buffer("BTCUSDT").snapshot().sample_count == 1


def test_concurrent_writes_are_thread_safe(base_time):
    """Hammer the buffer from multiple threads; final count must equal total adds."""
    buf = SlidingWindowBuffer(window_seconds=60)
    n_threads, adds_per_thread = 8, 50

    def worker():
        for i in range(adds_per_thread):
            buf.add(price=100.0 + i, quantity=1.0, timestamp=base_time)

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert buf.snapshot().sample_count == n_threads * adds_per_thread
