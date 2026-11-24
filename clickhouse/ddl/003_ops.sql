-- 사고·점검 기록 (v1 pipeline_incidents 와 같은 모양, docs/00 §2)
CREATE TABLE IF NOT EXISTS mp2.pipeline_incidents
(
    detected_at DateTime('UTC'),
    stage       LowCardinality(String),
    component   LowCardinality(String),
    severity    LowCardinality(String),
    title       String,
    detail      String,
    source      LowCardinality(String),
    dedup_key   String
)
ENGINE = ReplacingMergeTree(detected_at)
ORDER BY (stage, dedup_key)
TTL detected_at + INTERVAL 180 DAY;
