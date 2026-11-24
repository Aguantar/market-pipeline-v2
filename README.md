# market-pipeline-v2

Upbit·Binance 시세를 웹소켓으로 받아 **불변 파일(JSONL.gz)** 과 **ClickHouse** 두 곳에 쓰고, 변환은 전부 SQL(dbt)로 하는 파이프라인.
`cdc-realtime-pipeline`(v1: Kafka·Flink·Debezium) 을 284일 운영하고 측정한 뒤 다시 만든 것이다.

- 왜 다시 만들었나: `docs/00-v1-stack-consolidated.md` (v1 의 정의·측정·사고·결정 총정리)
- 설계: `docs/01-design.md` (원칙 4, 구조, 수집기 규약, 멱등 키, rebuild-from-files 합격선, 확장은 계약으로)

## 구조
수집기 3개 → `data/raw/<source>/<stream>/dt=…/hour=…/part-*.jsonl.gz` + `mp2.raw_*` (ReplacingMergeTree, 자연키) → dbt → 대조·헬스체크

## 실행
```
uv venv .venv && uv pip install --python .venv/bin/python websockets clickhouse-connect pytest pytest-asyncio dbt-core==1.11.6 dbt-clickhouse==1.10.0
cp .env.example .env                       # CLICKHOUSE_PASSWORD, AIRFLOW_*, (SLACK_WEBHOOK_URL)
docker compose up -d                       # clickhouse, 수집기 4, airflow(postgres·scheduler·webserver 127.0.0.1:8085)
for f in clickhouse/ddl/*.sql; do docker exec -i mp2-clickhouse clickhouse-client --password "$CLICKHOUSE_PASSWORD" --multiquery < $f; done
PYTHONPATH=. .venv/bin/python -m pytest -q # 단위·통합 테스트
CH_PASSWORD=... .venv/bin/python -m pipeline.rebuild --from 2026-11-23T23:54:00Z --to 2026-11-24T00:00:00Z   # 파일 → 재구성 → 라이브와 셀 대조
cd dbt && CH_PASSWORD=... ../.venv/bin/dbt build --profiles-dir .
```

## 운영
- 수집기 로그 `docker logs mp2-upbit-trades` 의 `[STATS]` 30초 (recv/parsed/ch inserted/dropped/lag p50·p95)
- Airflow: dbt_hourly · dbt_daily_finalize · reconcile_upbit · reconcile_binance · health_check
- 외부 감시: `scripts/ops/heartbeat-push.sh` (cron 5분) → Oracle cdc-watchdog
- 기록: `docs/worklog.md`
