from __future__ import annotations

from unittest.mock import patch

import pytest

from anomaly_detection.detector import AnomalyDetector, compute_zscore, evaluate
from tests.conftest import make_stats


class TestComputeZScore:
    def test_zscore_of_mean_is_zero(self):
        assert compute_zscore(price=100.0, mean=100.0, stdev=5.0) == 0.0

    def test_zscore_two_stdevs_above_mean(self):
        assert compute_zscore(price=110.0, mean=100.0, stdev=5.0) == 2.0

    def test_zero_stdev_returns_zero_not_divide_error(self):
        assert compute_zscore(price=500.0, mean=100.0, stdev=0.0) == 0.0


class TestEvaluate:
    def test_flags_price_spike_beyond_threshold(self):
        stats = make_stats(mean=100.0, stdev=2.0, count=20)
        is_anomaly, z = evaluate(price=107.0, stats=stats, threshold=3.0, min_samples=5)

        assert is_anomaly is True
        assert z == pytest.approx(3.5)

    def test_does_not_flag_price_within_threshold(self):
        stats = make_stats(mean=100.0, stdev=2.0, count=20)
        is_anomaly, z = evaluate(price=102.0, stats=stats, threshold=3.0, min_samples=5)

        assert is_anomaly is False
        assert z == pytest.approx(1.0)

    def test_flags_flash_crash_negative_deviation(self):
        stats = make_stats(mean=100.0, stdev=2.0, count=20)
        is_anomaly, z = evaluate(price=90.0, stats=stats, threshold=3.0, min_samples=5)

        assert is_anomaly is True
        assert z == pytest.approx(-5.0)

    def test_insufficient_samples_never_flags_regardless_of_deviation(self):
        stats = make_stats(mean=100.0, stdev=1.0, count=2)
        is_anomaly, z = evaluate(price=1000.0, stats=stats, threshold=3.0, min_samples=5)

        assert is_anomaly is False
        assert z == 0.0

    def test_exactly_at_threshold_is_not_flagged(self):
        """Boundary check: strict > threshold, not >=."""
        stats = make_stats(mean=100.0, stdev=1.0, count=20)
        is_anomaly, z = evaluate(price=103.0, stats=stats, threshold=3.0, min_samples=5)

        assert z == pytest.approx(3.0)
        assert is_anomaly is False


class TestAnomalyDetector:
    def test_check_tick_returns_none_when_within_normal_range(self, base_time):
        detector = AnomalyDetector(threshold=3.0, min_samples=5)
        stats = make_stats(mean=100.0, stdev=2.0, count=20)

        result = detector.check_tick("BTCUSDT", price=101.0, timestamp=base_time, stats=stats)

        assert result is None

    def test_check_tick_returns_event_and_logs_warning_on_flash_crash(self, base_time):
        detector = AnomalyDetector(threshold=3.0, min_samples=5)
        stats = make_stats(mean=60000.0, stdev=50.0, count=30)

        # Simulate a flash crash: price collapses 500 points below the rolling mean
        crash_price = 59500.0

        with patch("anomaly_detection.detector.log_with_context") as mock_log:
            event = detector.check_tick(
                "BTCUSDT", price=crash_price, timestamp=base_time, stats=stats
            )

        assert event is not None
        assert event.symbol == "BTCUSDT"
        assert event.triggering_price == crash_price
        assert event.deviation_zscore == pytest.approx(-10.0)
        mock_log.assert_called_once()
        # Verify the WARN-level structured log carried the required fields
        _, kwargs = mock_log.call_args
        assert kwargs["symbol"] == "BTCUSDT"
        assert kwargs["triggering_price"] == crash_price
        assert "deviation_zscore" in kwargs

    def test_mocked_stream_of_ticks_flags_only_the_spike(self, base_time):
        """End-to-end-ish test simulating a stream of ticks through the detector,
        without any real Kafka/network dependency (fully mocked stream)."""
        detector = AnomalyDetector(threshold=3.0, min_samples=5)

        # Pretend these WindowStats snapshots arrive in sequence for a symbol
        mocked_stream = [
            (100.0, make_stats(mean=100.0, stdev=1.0, count=10)),
            (100.5, make_stats(mean=100.1, stdev=1.0, count=11)),
            (99.7, make_stats(mean=100.05, stdev=1.0, count=12)),
            (150.0, make_stats(mean=100.1, stdev=1.0, count=13)),  # the spike
            (100.2, make_stats(mean=100.1, stdev=1.0, count=14)),
        ]

        anomalies = [
            detector.check_tick("ETHUSDT", price=price, timestamp=base_time, stats=stats)
            for price, stats in mocked_stream
        ]
        flagged = [a for a in anomalies if a is not None]

        assert len(flagged) == 1
        assert flagged[0].triggering_price == 150.0
