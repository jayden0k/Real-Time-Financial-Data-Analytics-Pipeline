from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from schemas.market_data import WindowStats


@pytest.fixture
def base_time() -> datetime:
    return datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def make_stats(mean: float, stdev: float, count: int = 10, window_seconds: float = 10.0) -> WindowStats:
    return WindowStats(
        vwap=mean,
        mean_price=mean,
        stdev_price=stdev,
        sample_count=count,
        window_seconds=window_seconds,
    )
