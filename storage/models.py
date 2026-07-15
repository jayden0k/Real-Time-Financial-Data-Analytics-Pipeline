"""
Row schema for the processed-ticks time-series table.

SQLite has no native partitioning, so we *simulate* time-series partitioning
the way you would on a constrained engine: a `partition_key` column
(`YYYY-MM-DD`) derived from `trade_time`, combined with a composite index on
(symbol, partition_key, trade_time). Range-scoped queries filter on
partition_key first, which is exactly the access pattern TimescaleDB's
hypertable chunking optimizes for automatically -- so swapping the writer
for a real TimescaleWriter later requires no query-shape changes.
"""

from __future__ import annotations

CREATE_TICKS_TABLE = """
CREATE TABLE IF NOT EXISTS processed_ticks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL,
    price           REAL NOT NULL,
    quantity        REAL NOT NULL,
    trade_time      TEXT NOT NULL,      -- ISO-8601 UTC
    side            TEXT NOT NULL,
    vwap            REAL NOT NULL,
    mean_price      REAL NOT NULL,
    stdev_price     REAL NOT NULL,
    sample_count    INTEGER NOT NULL,
    is_anomaly      INTEGER NOT NULL DEFAULT 0,
    anomaly_zscore  REAL,
    partition_key   TEXT NOT NULL       -- YYYY-MM-DD, simulates day-partitioning
);
"""

CREATE_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_ticks_symbol_partition_time
    ON processed_ticks (symbol, partition_key, trade_time);
CREATE INDEX IF NOT EXISTS idx_ticks_anomaly
    ON processed_ticks (is_anomaly)
    WHERE is_anomaly = 1;
"""

INSERT_TICK = """
INSERT INTO processed_ticks
    (symbol, price, quantity, trade_time, side, vwap, mean_price,
     stdev_price, sample_count, is_anomaly, anomaly_zscore, partition_key)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""
