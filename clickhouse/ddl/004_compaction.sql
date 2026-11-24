-- 압축(JSONL.gz → Parquet) 실행 기록. 2주차 Spark 와 같은 작업을 나란히 재는 기준선 (docs/01-design §7).
CREATE TABLE IF NOT EXISTS mp2.compaction_runs
(
    run_at       DateTime('UTC'),
    engine       LowCardinality(String),   -- clickhouse | spark
    source       LowCardinality(String),
    stream       LowCardinality(String),
    day          Date,
    src_files    UInt32,
    src_rows     UInt64,
    src_bytes    UInt64,                   -- 입력 gz 바이트
    out_rows     UInt64,
    out_bytes    UInt64,                   -- Parquet 바이트
    verified     UInt8,                    -- 행수·해시 일치
    seconds      Float32,
    peak_mem_mb  Float32,
    note         String
)
ENGINE = MergeTree
ORDER BY (day, source, stream, engine, run_at);
