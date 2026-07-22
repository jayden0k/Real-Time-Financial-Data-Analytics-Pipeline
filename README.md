# Real-Time Financial Data Analytics Pipeline

Production-style, containerized market-data pipeline: **WebSocket ingestion → Kafka →
async stream processing (VWAP) → statistical anomaly detection → batched time-series storage.**

Built to demonstrate the engineering patterns expected of a financial-data platform:
Producer-Consumer decoupling via Kafka, backpressure-safe batching, thread-safe shared
state, strict typing/validation at every boundary, structured logging, and testable
business logic isolated from I/O.

## Architecture

```
                     ┌─────────────────────┐
                     │   Binance WS Feed    │  (public, free, no auth)
                     └──────────┬───────────┘
                                │ raw JSON ticks
                     ┌──────────▼───────────┐
                     │  INGESTION SERVICE    │  ingestion/
                     │  asyncio websocket    │  - exponential backoff reconnect
                     │  client + Kafka       │  - graceful shutdown (SIGINT/SIGTERM)
                     │  producer             │  - Pydantic validation before publish
                     └──────────┬───────────┘
                                │ produces
                     ┌──────────▼───────────┐
                     │  Kafka: market-data- │
                     │  raw (topic)          │
                     └──────────┬───────────┘
                                │ consumes
                     ┌──────────▼───────────┐
                     │  PROCESSING SERVICE   │  processing/
                     │  async Kafka consumer │  - UTC normalization
                     │  + SlidingWindowBuffer│  - thread-safe deque per symbol
                     │  (VWAP, mean, stdev)  │  - 10s rolling window
                     └──────────┬───────────┘
                                │ enriched tick + stats
                     ┌──────────▼───────────┐
                     │  ANOMALY DETECTION    │  anomaly_detection/
                     │  z-score vs rolling   │  - flash-crash / spike detection
                     │  mean/stdev           │  - structured WARN logs
                     └──────────┬───────────┘
                                │ processed tick (+ anomaly flag)
                     ┌──────────▼───────────┐
                     │  STORAGE WRITER       │  storage/
                     │  batched bulk insert  │  - queue + background flush thread
                     │  (SQLite TS-partition,│  - swappable for TimescaleDB
                     │   Timescale-ready)    │    via same interface (Strategy)
                     └───────────────────────┘
```

## Directory layout

```
fintech-pipeline/
├── docker-compose.yml          # Kafka, Zookeeper, TimescaleDB, Adminer
├── requirements.txt
├── .env.example
├── config/
│   └── settings.py             # Singleton, env-driven configuration
├── schemas/
│   └── market_data.py          # Pydantic models = schema contract at every hop
├── common/
│   └── logging_config.py       # Structured JSON logging setup
├── ingestion/
│   ├── websocket_client.py     # asyncio WS client, backoff, graceful shutdown
│   ├── kafka_producer.py       # aiokafka producer wrapper
│   └── main.py                 # ingestion-service entrypoint
├── processing/
│   ├── sliding_window.py       # thread-safe rolling window (VWAP/mean/stdev)
│   ├── normalizer.py           # strict parsing + UTC normalization
│   ├── kafka_consumer.py       # aiokafka consumer wrapper
│   └── main.py                 # processing-service entrypoint
├── anomaly_detection/
│   └── detector.py             # pure, testable z-score anomaly logic
├── storage/
│   ├── models.py                # table DDL / row schema
│   └── db_writer.py             # batched writer (Strategy: SQLite / Timescale)
└── tests/
    ├── conftest.py
    ├── test_sliding_window.py
    └── test_anomaly_detection.py
```

## Design patterns used (and why)

| Pattern | Where | Why |
|---|---|---|
| **Producer-Consumer** | Kafka between ingestion and processing | Decouples ingestion rate from processing rate; either side can restart, scale, or fail independently without data loss (Kafka retains the log). |
| **Singleton (via `lru_cache`)** | `config/settings.py` | One canonical, validated config object per process; avoids re-parsing env vars and avoids config drift between modules. |
| **Strategy** | `storage/db_writer.py` | `TimeSeriesWriter` is an abstract interface; `SQLiteWriter` is the concrete implementation shipped here, but a `TimescaleWriter` can be dropped in with zero changes to calling code. |
| **Repository-ish batching** | `storage/db_writer.py` | Writes are buffered in a queue and flushed in batches on a timer/size threshold — avoids per-tick I/O, which would bottleneck under real market load. |
| **Pure functions for business logic** | `anomaly_detection/detector.py` | The anomaly rule takes primitives in, returns a primitive result out — no I/O, no Kafka, no DB. This is what makes it trivially unit-testable. |
| **Fail-fast validation at the boundary** | `schemas/market_data.py` | Every tick is validated into a Pydantic model *once*, immediately after leaving the wire/Kafka. Downstream code trusts the type system instead of re-checking dict keys everywhere. |

## Running it

```bash
cp .env.example .env
docker compose up -d          # Kafka, Zookeeper, TimescaleDB, Adminer
pip install -r requirements.txt

# Terminal 1 — ingestion service (WS -> Kafka)
python -m ingestion.main

# Terminal 2 — processing service (Kafka -> VWAP -> anomaly -> storage)
python -m processing.main

# Tests (no Kafka/network required — pure unit tests)
pytest -v
```

Data source: Binance's public trade WebSocket (`wss://stream.binance.com:9443`) —
free, unauthenticated, no API key required, so the whole pipeline runs end-to-end
with zero paid dependencies.
