# market-pipeline-v2

Upbit·Binance 시세를 웹소켓으로 받아 **불변 파일(JSONL.gz)** 과 **ClickHouse** 두 곳에 쓰고, 파싱은 ClickHouse Materialized View 한 곳에서, 변환은 전부 SQL(dbt)로 하는 파이프라인.
`cdc-realtime-pipeline`(v1: MySQL·Debezium·Kafka·Flink) 을 284일 운영하고 측정한 뒤, 2026-11-20 정전 복구에 사람 4시간이 든 것을 계기로 다시 만들었다.

- 왜 다시 만들었나: `docs/00-v1-stack-consolidated.md` (v1 의 정의·측정·사고 18건·결정 총정리, 출처 표기)
- 설계: `docs/01-design.md` (원칙 4, 봉투 계약, 멱등 키, rebuild-from-files 합격선, §10 결정 기록 12항)
- 전·후 비교: `docs/02-v1-v2-comparison.md` (v1 열 채움, v2 열은 7일 관찰 뒤 2026-12-02; §7 관찰 중 발견 5건)
- 변경 연습: `docs/03-schema-version-exercise.md` (거래소 형식이 바뀌면 어디를 어떤 순서로)
- 시간순 기록: `docs/worklog.md`

## 구조

```
Upbit WS ─┐                 ┌─ data/raw/<source>/<stream>/dt=…/hour=…/part-*.jsonl.gz   (원본 봉투, 불변, fsync→rename)
          ├─ 수집기 4 ──────┤
Binance WS┘  (파싱 안 함)   └─ mp2.envelopes (Null) ─MV 4(sql_parse)─▶ mp2.raw_* (ReplacingMergeTree, 자연키)
                                                                          │
                       dbt: staging(FINAL) → 1분 집계(insert_overwrite 일 파티션) → 거래소 대조 → 품질
                                                                          │
  매일 00:20  파일 → 파싱 → 컬럼형 Parquet ─ 라이브 raw 와 (cell, hour) 행수·cityHash64 대조 ─▶ 01:20 Oracle rsync (14일)
```

- 봉투 = `{source, stream, schema_version, recv_ts, payload(원문)}`. 수집기는 거래소 원문을 그대로 두 곳에 쓴다.
- 파서의 단일 출처는 `pipeline/sql_parse.py`. 라이브 MV·rebuild·컬럼형 내보내기가 같은 SELECT 를 쓴다. `clickhouse/ddl/005_envelopes_mv.sql` 은 그것으로 **생성**된 파일이다(테스트가 일치를 강제).
- 금액·수량은 JSON 원문 텍스트 → Decimal(20,8). Float 를 거치지 않는다(v1 교훈: 합계 2e-8 오차).
- 파일이 1순위다. "파일에서 전부 다시 만들어 라이브와 셀 단위로 같다" 를 보이는 `pipeline.rebuild` 가 첫 산출물이고, 매일 00:20 의 내보내기가 같은 증명을 하루치로 반복한다.

## 실행

```
uv venv .venv && uv pip install --python .venv/bin/python websockets clickhouse-connect pyarrow pytest pytest-asyncio dbt-core==1.11.6 dbt-clickhouse==1.10.0
cp .env.example .env                       # CLICKHOUSE_PASSWORD, AIRFLOW_POSTGRES_PASSWORD, AIRFLOW_ADMIN_PASSWORD, AIRFLOW_FERNET_KEY
docker compose up -d                       # clickhouse, 수집기 4, airflow(postgres·init·scheduler·webserver 127.0.0.1:8085)
for f in clickhouse/ddl/*.sql; do docker exec -i mp2-clickhouse clickhouse-client --password "$CLICKHOUSE_PASSWORD" --multiquery < $f; done
.venv/bin/python -m pytest -q              # 단위 19 + 통합(ClickHouse 필요) 3
CH_PASSWORD=… .venv/bin/python -m pipeline.rebuild --from 2026-11-24T00:10:00Z --to 2026-11-24T01:10:00Z   # 파일 → envelopes → MV → 라이브와 셀 대조 (1시간 246만 행 70초)
CH_PASSWORD=… .venv/bin/python -m pipeline.parity --day 2026-11-24                                       # 파이썬 파서 vs SQL 파서 동치
cd dbt && CH_PASSWORD=… ../.venv/bin/dbt build --profiles-dir .                                            # 21/21
```

## 운영

| 무엇 | 어디 | 주기 |
|---|---|---|
| 수집기 상태 | `docker logs mp2-upbit-trades` 의 `[STATS]` (recv/ch inserted/dropped/lag p50·p95) | 30초 |
| Airflow DAG 7 | dbt_hourly(매시 05) · dbt_daily_finalize(00:30) · reconcile_binance(00:40) · reconcile_upbit(06:35) · health_check(10분) · export_columnar(00:20) · backup_daily(01:20) | UTC |
| 외부 감시 | `scripts/ops/heartbeat-push.sh` → Oracle cdc-watchdog (세 스트림 모두 0 일 때만 stale, 알림 카나리 26h 부재 → degraded) | 5분 |
| 알림 카나리 | `alert_canary` DAG 가 실제 경로로 Slack 1건 → 마커 → heartbeat → 감시자 (`docs/06`) | 매일 07:10 UTC |
| 관측 지표 | `scripts/observe/collect_metrics.sh` → `mp2.ops_metrics_5m` + CSV (v1 과 같은 단위: 5분 행수, lag p50/p95, 신선도, RSS, 디스크) | 5분 |
| 측정 | `mp2.compaction_runs` (엔진별 하루치 재파싱 시간·메모리·크기), `mp2.pipeline_incidents`, `mp2.collector_gaps` | |
| CD | `~/mp2-bin/mp2-cd`(Go, `cd/`) — GHCR digest 변화 감지 → `MODE=notify` 알림 / `MODE=deploy` 수집기 4개 pull·up·검증·롤백 (`docs/05`) | 5분 |

컨테이너 8개, 호스트 메모리 약 4GB (v1: 26개·약 10GB). 자원 수치는 `docs/02`.

## 디렉터리

```
pipeline/        envelope · filesink(조각 회전·fsync·복구) · chsink(배처) · schema(표·멱등 키) · sql_parse(파서 단일 출처) · parse(파이썬 참조 파서)
pipeline/collectors/  base(Sink·Connection·gap 기록) · upbit · binance
pipeline/rebuild/     파일 → 재구성 → 셀 대조        pipeline/compact.py  하루치 컬럼형 Parquet + 대조        pipeline/parity.py  파서 동치
clickhouse/ddl/  001 raw · 002 reference(캔들) · 003 ops(incidents) · 004 compaction_runs · 005 envelopes+MV(생성) · 006 ops_metrics
dbt/             staging 4 · intermediate 5 · quality 4 · tests
airflow/dags/    위 DAG 7 + mp2_common        airflow/plugins/  ClickHouse hook · Slack 콜백
cd/              main.go — pull 방식 배포기 (docs/05)
spark/           export_day_spark.py — compact 와 같은 입력·출력·검증을 Spark(local) 로. 학습 + 이 규모에서 지는 지점 기록용 (미실행)
scripts/         ops/heartbeat-push.sh · observe/collect_metrics.sh
tests/           filesink · parse · chsink(통합) · sql_parse(생성 DDL 일치)
```

## 지금 상태 (2026-11-25)

1주차 구현 전부 가동. **7일 무변경 관찰** 재시작 2026-11-25 10:25:33 UTC(첫 이틀에 결함 8건 발견·7건 수정, `docs/02` §7) ~ 12-02 — 이 기간 코드·DDL 을 바꾸지 않는다. 관찰 뒤: 전원 뽑기 리허설, `docs/02` v2 열, `exercise/schema-v2` 브랜치 반영, Spark 1회 실행. 원본 JSONL 7.9GB/일의 보존 기간은 미결(`docs/01` §8).
