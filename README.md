# market-pipeline-v2

Upbit·Binance 시세를 웹소켓으로 받아 **불변 파일(JSONL.gz)** 과 **ClickHouse** 두 곳에 쓰고, 변환은 전부 SQL(dbt)로 하는 파이프라인.
`cdc-realtime-pipeline`(v1: Kafka·Flink·Debezium) 을 284일 운영하고 측정한 뒤 다시 만든 것이다.

- 왜 다시 만들었나: `docs/00-v1-stack-consolidated.md` (v1 의 정의·측정·사고·결정 총정리)
- 설계: `docs/01-design.md` (원칙 4, 구조, 수집기 규약, 멱등 키, rebuild-from-files 합격선, 확장은 계약으로)

## 구조
수집기 3개 → `data/raw/<source>/<stream>/dt=…/hour=…/part-*.jsonl.gz` + `mp2.raw_*` (ReplacingMergeTree, 자연키) → dbt → 대조·헬스체크

## 실행
```
uv venv .venv && uv pip install --python .venv/bin/python websockets clickhouse-connect pytest pytest-asyncio
cp .env.example .env   # CLICKHOUSE_PASSWORD
docker compose up -d clickhouse
docker exec -i mp2-clickhouse clickhouse-client --password "$CLICKHOUSE_PASSWORD" --multiquery < clickhouse/ddl/001_raw.sql
PYTHONPATH=. .venv/bin/python -m pytest -q
```
