-- raw 테이블 (docs/01-design §4). 전부 ReplacingMergeTree(recv_ts) + 자연키 → 같은 행을 다시 넣어도 멱등.
-- 금액·수량은 Decimal (v1 교훈 docs/00 §7: Float64 합계 2e-8 오차). 호가 배열은 Float64 (v1 과 같음, 집계 입력).
CREATE DATABASE IF NOT EXISTS mp2;

CREATE TABLE IF NOT EXISTS mp2.raw_upbit_trades
(
    market          LowCardinality(String),
    trade_ts        DateTime64(3, 'UTC'),
    sequential_id   UInt64,
    price           Decimal(20, 8),
    volume          Decimal(20, 8),
    ask_bid         LowCardinality(String),
    best_ask_price  Nullable(Decimal(20, 8)),
    best_ask_size   Nullable(Decimal(20, 8)),
    best_bid_price  Nullable(Decimal(20, 8)),
    best_bid_size   Nullable(Decimal(20, 8)),
    exchange_ts     DateTime64(3, 'UTC'),
    recv_ts         DateTime64(3, 'UTC'),
    inserted_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(recv_ts)
PARTITION BY toDate(trade_ts)
ORDER BY (market, sequential_id, trade_ts)
TTL toDateTime(trade_ts) + INTERVAL 400 DAY;

CREATE TABLE IF NOT EXISTS mp2.raw_upbit_orderbook
(
    market          LowCardinality(String),
    ts              DateTime64(3, 'UTC'),
    level           Float64,
    total_ask_size  Float64,
    total_bid_size  Float64,
    ask_prices      Array(Float64),
    ask_sizes       Array(Float64),
    bid_prices      Array(Float64),
    bid_sizes       Array(Float64),
    stream_type     LowCardinality(String),
    recv_ts         DateTime64(3, 'UTC'),
    inserted_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(recv_ts)
PARTITION BY toDate(ts)
ORDER BY (market, ts)
TTL toDateTime(ts) + INTERVAL 14 DAY;

CREATE TABLE IF NOT EXISTS mp2.raw_binance_trades
(
    symbol          LowCardinality(String),
    trade_id        UInt64,
    trade_ts        DateTime64(3, 'UTC'),
    price           Decimal(20, 8),
    qty             Decimal(20, 8),
    is_buyer_maker  UInt8,
    event_ts        DateTime64(3, 'UTC'),
    recv_ts         DateTime64(3, 'UTC'),
    inserted_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(recv_ts)
PARTITION BY toDate(trade_ts)
ORDER BY (symbol, trade_id)
TTL toDateTime(trade_ts) + INTERVAL 60 DAY;

CREATE TABLE IF NOT EXISTS mp2.raw_binance_depth
(
    symbol          LowCardinality(String),
    kind            LowCardinality(String),     -- diff | snapshot
    first_update_id UInt64,
    last_update_id  UInt64,
    event_ts        DateTime64(3, 'UTC'),
    bid_prices      Array(Float64),
    bid_sizes       Array(Float64),
    ask_prices      Array(Float64),
    ask_sizes       Array(Float64),
    recv_ts         DateTime64(3, 'UTC'),
    inserted_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(recv_ts)
PARTITION BY toDate(event_ts)
ORDER BY (symbol, last_update_id, kind)
TTL toDateTime(event_ts) + INTERVAL 14 DAY;

-- 수집기 재연결·공백 기록 (docs/01-design §3)
CREATE TABLE IF NOT EXISTS mp2.collector_gaps
(
    source      LowCardinality(String),
    stream      LowCardinality(String),
    conn        String,
    from_ts     DateTime64(3, 'UTC'),
    to_ts       DateTime64(3, 'UTC'),
    reason      String,
    inserted_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (source, stream, from_ts);
