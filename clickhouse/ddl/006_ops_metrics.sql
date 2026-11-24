-- 5분 관측 지표. v1 scripts/observe/collect_metrics.sh(metrics_5m.csv) 와 같은 단위로 적어 v1/v2 전·후 비교에 쓴다.
-- 관측자다. 파이프라인 경로(수집기·MV·dbt)를 바꾸지 않는다. 2026-11-24 추가.
CREATE TABLE IF NOT EXISTS mp2.ops_metrics_5m
(
    ts                DateTime('UTC'),
    stream            LowCardinality(String),   -- 'upbit/trades' … 또는 'host'
    rows_5m           UInt64,                   -- 최근 5분 수신 행수 (recv_ts 기준)
    keys_5m           UInt32,                   -- 마켓/심볼 수
    lag_p50_ms        Float32,                  -- recv_ts - 이벤트 시각
    lag_p95_ms        Float32,
    lag_max_ms        Float32,
    freshness_ms      Float32,                 -- 스냅샷 시각 - max(recv_ts): 표 신선도. v1 e2e(이벤트→조회가능)의 상한 근사
    gaps_1h           UInt32,                   -- collector_gaps 최근 1시간
    container_mem_mb  Float32,                  -- 해당 수집기 컨테이너 RSS
    ch_mem_mb         Float32,                  -- host 행만: ClickHouse 컨테이너 RSS
    host_used_mb      Float32,
    disk_free_gb      Float32,
    raw_dir_gb        Float32,                  -- data/raw
    parquet_dir_gb    Float32                   -- data/parquet
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (stream, ts)
TTL ts + INTERVAL 400 DAY;
