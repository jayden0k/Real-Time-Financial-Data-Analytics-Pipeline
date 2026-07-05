"""
Anomaly (flash-crash / price-spike) detection.

Design note: `evaluate()` is a pure function -- it takes primitives in and
returns a primitive/dataclass result out, with no Kafka, DB, or logging side
effects buried inside it. That's deliberate: pure functions are what make
business rules unit-testable in isolation, without spinning up brokers or
mocking half the world. Logging is a *caller's* responsibility (see
`AnomalyDetector.check_tick`), keeping the rule itself side-effect-free.
"""

from __future__ import annotations

import logging
from datetime import datetime

from common.logging_config import log_with_context
from schemas.market_data import AnomalyEvent, WindowStats

logger = logging.getLogger(__name__)


def compute_zscore(price: float, mean: float, stdev: float) -> float:
    """Standard z-score. Returns 0.0 when stdev is 0 (flat/insufficient data)
    to avoid division-by-zero false positives on a perfectly flat window.
    """
    if stdev <= 0:
        return 0.0
    return (price - mean) / stdev


def evaluate(
    price: float,
    stats: WindowStats,
    threshold: float,
    min_samples: int,
) -> tuple[bool, float]:
    """Return (is_anomaly, zscore) for a single price against rolling stats.

    Anomaly rule: |z-score| > threshold, but only once the window has enough
    samples to make the mean/stdev statistically meaningful -- otherwise the
    very first tick in an empty window would always register as a 0-stdev
    "anomaly" or the second tick would swing wildly on n=1 stats.
    """
    if stats.sample_count < min_samples:
        return False, 0.0

    z = compute_zscore(price, stats.mean_price, stats.stdev_price)
    return abs(z) > threshold, z


class AnomalyDetector:
    """Stateful wrapper: applies `evaluate()` and emits structured WARN logs
    plus an AnomalyEvent when a tick crosses the threshold.
    """

    def __init__(self, threshold: float, min_samples: int) -> None:
        self._threshold = threshold
        self._min_samples = min_samples

    def check_tick(
        self, symbol: str, price: float, timestamp: datetime, stats: WindowStats
    ) -> AnomalyEvent | None:
        is_anomaly, zscore = evaluate(
            price=price,
            stats=stats,
            threshold=self._threshold,
            min_samples=self._min_samples,
        )
        if not is_anomaly:
            return None

        event = AnomalyEvent(
            symbol=symbol,
            timestamp=timestamp,
            triggering_price=price,
            rolling_mean=stats.mean_price,
            rolling_stdev=stats.stdev_price,
            deviation_zscore=zscore,
            threshold=self._threshold,
        )

        log_with_context(
            logger,
            logging.WARNING,
            "Anomaly detected: price deviation exceeds threshold",
            symbol=event.symbol,
            timestamp=event.timestamp.isoformat(),
            triggering_price=event.triggering_price,
            deviation_zscore=round(event.deviation_zscore, 3),
            rolling_mean=round(event.rolling_mean, 4),
            rolling_stdev=round(event.rolling_stdev, 4),
            threshold=event.threshold,
        )

        return event
