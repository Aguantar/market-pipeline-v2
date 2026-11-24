-- 참조 테이블: 거래소 캔들(진실값). Airflow reconcile_* 가 REST 로 채운다. ReplacingMergeTree(fetched_at) 라 재적재 멱등.
CREATE TABLE IF NOT EXISTS mp2.upbit_hourly_candles
(
    market     LowCardinality(String),
    hour_utc   DateTime('UTC'),
    open       Float64, high Float64, low Float64, close Float64,
    volume     Float64,
    amount     Float64,
    fetched_at DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY (market, hour_utc)
TTL hour_utc + INTERVAL 90 DAY;

CREATE TABLE IF NOT EXISTS mp2.binance_hourly_candles
(
    symbol       LowCardinality(String),
    hour_utc     DateTime('UTC'),
    open         Float64, high Float64, low Float64, close Float64,
    volume       Float64,
    quote_volume Float64,
    trade_count  UInt32,
    fetched_at   DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY (symbol, hour_utc)
TTL hour_utc + INTERVAL 90 DAY;
