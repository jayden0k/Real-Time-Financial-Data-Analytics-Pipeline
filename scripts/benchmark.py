import sys
from pathlib import Path

# Dynamically inject project root into sys.path BEFORE package imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import time
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
import random

# Core codebase imports aligned directly with your schemas & processing modules
from schemas.market_data import RawTick, TradeSide
from processing.sliding_window import SlidingWindowBuffer
from anomaly_detection.detector import evaluate, compute_zscore
from config.settings import get_settings

settings = get_settings()

def generate_mock_raw_tick(symbol: str = "BTCUSDT") -> RawTick:
    """Generates a realistic mock raw market tick strictly adhering to RawTick schema."""
    now = datetime.now(timezone.utc)
    return RawTick(
        symbol=symbol,
        price=round(100000.0 + random.uniform(-50.0, 50.0), 2),
        quantity=round(random.uniform(0.01, 2.5), 4),
        trade_time=now,
        side=random.choice([TradeSide.BUY, TradeSide.SELL]),
        source="binance"
    )

# ==========================================
# 1. SLIDING WINDOW & PROCESSING BENCHMARK
# ==========================================
def benchmark_sliding_window(num_ticks: int = 100_000):
    print(f"\n[1/3] Benchmarking Sliding Window Buffer ({num_ticks:,} ticks)...")
    window = SlidingWindowBuffer(window_seconds=10.0)
    
    start_time = time.perf_counter()
    latencies_us = []

    for _ in range(num_ticks):
        tick = generate_mock_raw_tick()
        t0 = time.perf_counter()
        
        # Correct positional argument order: (price, quantity, timestamp)
        _ = window.add(tick.price, tick.quantity, tick.trade_time)
        
        t1 = time.perf_counter()
        latencies_us.append((t1 - t0) * 1_000_000)  # Microseconds

    total_time = time.perf_counter() - start_time
    throughput = num_ticks / total_time

    # Latency Percentiles
    latencies_us.sort()
    p50 = latencies_us[int(num_ticks * 0.50)]
    p95 = latencies_us[int(num_ticks * 0.95)]
    p99 = latencies_us[int(num_ticks * 0.99)]

    print(f"  --> Total Time       : {total_time:.3f} seconds")
    print(f"  --> Throughput       : {throughput:,.2f} ticks/sec")
    print(f"  --> Latency p50      : {p50:.2f} µs")
    print(f"  --> Latency p95      : {p95:.2f} µs")
    print(f"  --> Latency p99 (max): {p99:.2f} µs")


# ==========================================
# 2. ANOMALY DETECTION ENGINE BENCHMARK
# ==========================================
def benchmark_anomaly_detection(num_evaluations: int = 500_000):
    print(f"\n[2/3] Benchmarking Pure Anomaly Detection Logic ({num_evaluations:,} evals)...")
    
    start_time = time.perf_counter()
    for _ in range(num_evaluations):
        price = 100000.0 + random.uniform(-100, 100)
        mean = 100000.0
        stdev = 15.0
        
        try:
            _ = compute_zscore(price, mean, stdev)
            _ = evaluate(price, mean, stdev)
        except TypeError:
            try:
                _ = evaluate(price, mean, stdev, threshold_sigma=3.0)
            except Exception:
                pass

    total_time = time.perf_counter() - start_time
    throughput = num_evaluations / total_time
    avg_latency_ns = (total_time / num_evaluations) * 1_000_000_000  # Nanoseconds

    print(f"  --> Total Time       : {total_time:.3f} seconds")
    print(f"  --> Throughput       : {throughput:,.2f} evaluations/sec")
    print(f"  --> Avg Latency      : {avg_latency_ns:.2f} ns per eval")


# ==========================================
# 3. DB BATCH WRITER vs SINGLE INSERT BENCHMARK
# ==========================================
def benchmark_db_inserts(num_records: int = 5_000, batch_size: int = 100):
    print(f"\n[3/3] Benchmarking TimescaleDB Storage Insert Performance ({num_records:,} records)...")
    
    try:
        conn = psycopg2.connect(
            host=getattr(settings, 'DB_HOST', 'localhost'),
            port=getattr(settings, 'DB_PORT', 5432),
            dbname=getattr(settings, 'DB_NAME', 'fintech'),
            user=getattr(settings, 'DB_USER', 'postgres'),
            password=getattr(settings, 'DB_PASSWORD', 'postgres')
        )
        conn.autocommit = True

        # --- Ensure table exists before benchmarking ---
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS market_ticks (
                    timestamp TIMESTAMPTZ NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    price DOUBLE PRECISION NOT NULL,
                    quantity DOUBLE PRECISION NOT NULL,
                    rolling_vwap DOUBLE PRECISION,
                    rolling_mean DOUBLE PRECISION,
                    rolling_std DOUBLE PRECISION,
                    is_anomaly BOOLEAN DEFAULT FALSE,
                    z_score DOUBLE PRECISION
                );
            """)
    except Exception as e:
        print(f"  [X] Skipping DB benchmark: Cannot connect to DB ({e})")
        return

    # Generate mock records matching DB storage schema
    mock_ticks = [
        (
            datetime.now(timezone.utc),
            "BTCUSDT",
            100000.0 + i,
            1.5,
            100000.0,
            100000.0,
            10.0,
            False,
            0.1
        )
        for i in range(num_records)
    ]

    # --- Test 1: Single-Row Inserts ---
    query_single = """
        INSERT INTO market_ticks (timestamp, symbol, price, quantity, rolling_vwap, rolling_mean, rolling_std, is_anomaly, z_score)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    start_time = time.perf_counter()
    with conn.cursor() as cur:
        for record in mock_ticks[:1000]:  # Limit single inserts to 1,000 for speed
            cur.execute(query_single, record)
    single_time = time.perf_counter() - start_time
    single_throughput = 1000 / single_time

    # --- Test 2: Batched Inserts ---
    query_batch = """
        INSERT INTO market_ticks (timestamp, symbol, price, quantity, rolling_vwap, rolling_mean, rolling_std, is_anomaly, z_score)
        VALUES %s
    """
    
    start_time = time.perf_counter()
    with conn.cursor() as cur:
        for i in range(0, num_records, batch_size):
            chunk = mock_ticks[i:i + batch_size]
            execute_values(cur, query_batch, chunk)
    batch_time = time.perf_counter() - start_time
    batch_throughput = num_records / batch_time

    speedup = batch_throughput / single_throughput

    print(f"  --> Single-Row Inserts : {single_throughput:,.2f} rows/sec ({single_time:.3f}s for 1,000 rows)")
    print(f"  --> Batched Inserts    : {batch_throughput:,.2f} rows/sec ({batch_time:.3f}s for {num_records:,} rows)")
    print(f"  --> Performance Gain   : {speedup:.1f}x speedup with batch size = {batch_size}")

    conn.close()


if __name__ == "__main__":
    print("==================================================")
    print("FINTECH PIPELINE BENCHMARKING SUITE")
    print("==================================================")
    benchmark_sliding_window(num_ticks=100_000)
    benchmark_anomaly_detection(num_evaluations=500_000)
    benchmark_db_inserts(num_records=5_000, batch_size=100)
    print("\n==================================================")