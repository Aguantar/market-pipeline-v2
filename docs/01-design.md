# 61. v2 설계 - 현재 규모에 맞게, 확장은 계약으로 (초안 2026-11-24)

- 목표: v1 이 증명한 것(값이 맞다는 것을 아는 방법)을 유지하면서, v1 이 못 한 것(혼자 살아남기, 재처리 = 라이브)을 설계로 해결한다.
- 비목표: 처리량 과시, 분산 처리 재현, 실시간 알림 1초 미만. 유입은 수백~1,000 events/s 이고 그것을 전제로 짠다(docs/31 §2).
- 결정 근거는 docs/59 §6·§7, docs/60(v1 총정리). 각 결정 옆에 번호를 붙인다.

## 1. 원칙 (docs/59 §2)

1. 원본은 파일로 영구 보관. 재처리 원천은 "Kafka 보존 24h" 가 아니라 파일 전부 (docs/58: Binance 호가 82분을 못 메운 이유).
2. 변환은 전부 ClickHouse SQL·이벤트타임. 스트림 상태가 없으면 재처리 = 라이브 (docs/58 §5 의 비결정성 2건이 생길 자리가 없다).
3. 모든 테이블 멱등. 원본은 ReplacingMergeTree 자연키, 집계는 파티션 교체 (docs/58 §4: orderbook_1m 은 MergeTree 라 사람이 중복을 막았다).
4. 프로세스 하나에 관심사 하나, 재부팅 뒤 사람 0분 (docs/58 §6: 4시간). **(11-24 변경)** 수집기는 systemd 유닛이 아니라 compose `restart: unless-stopped` 로 올린다. 이 호스트에 사용자 systemd 세션이 없고 시스템 유닛은 설치마다 sudo 가 필요해 반복이 느리다. v1 정전 때 23컨테이너가 restart 정책으로 전부 자동 복귀한 실측이 있어 목표(사람 0분)엔 차이가 없다. 부팅 점검 oneshot 하나만 systemd 로.

## 2. 구조

```
Upbit WS ─┐                      ┌→ raw/ (JSONL.gz, 시간 조각)  ──→ [야간] Parquet 일 단위 압축 ──→ Oracle 블록볼륨(압축 사본)
Binance WS┼→ 수집기 3개(systemd) ─┤                                        ↑
          │                      └→ ClickHouse async insert ──→ dbt(staging→intermediate→marts→quality) ──→ 대조·헬스체크·digest
          └────────────────────── 재연결·공백 기록 → collector_gaps
Airflow: reconcile_*, dbt_daily, compact_raw(파일→Parquet), backup_daily, health_check
[2주차 측정] Spark 배치: 파일 재처리·대조·압축.  Iceberg: Parquet 폴더 대비 측정 뒤 결정.
```

빠진 것: Kafka·ZooKeeper, Flink, Debezium·MySQL(원장은 Oracle Postgres 로 이미 이관, docs/59 §3), Prometheus·statsd·kafka-ui.

## 3. 수집기 (프로세스 4개 = 스트림 1개씩, Python, compose)

| 수집기 | 소스 | v1 실측 유입 | 출력 |
|---|---|---|---|
| upbit-trades | Upbit WS trade (KRW 전 마켓 ~300) | ~25 rows/s (docs/58 collector STATS) | raw/upbit/trades |
| upbit-orderbook | Upbit WS orderbook (300마켓) | 200~400 msg/s (docs/58) | raw/upbit/orderbook |
| binance | Binance WS aggTrade(500+ 심볼) + depth(10심볼) | 250 + 70 msg/s (docs/58) | raw/binance/trades, raw/binance/depth |

공통 규약(이것이 확장 계약이다, §9):
- 메시지 봉투: `{source, stream, schema_version, recv_ts, payload}`. payload 는 거래소 원문 그대로. 파서는 수집기에 없다 → 수집기는 "받아서 두 곳에 쓴다" 만 한다. **(11-24 구현)** ClickHouse 쪽도 봉투를 받는다: `envelopes`(Null 엔진) → Materialized View 4개(`pipeline/sql_parse.py` 가 단일 출처, DDL 005 는 생성물)가 raw_* 로 파싱. rebuild 는 파일을 file() 로 같은 envelopes 에 넣어 **라이브와 같은 파서**를 지난다. 파이썬 parse.py 는 테스트용이며 `pipeline/parity.py` 가 4스트림 227만 행에서 셀 단위 동치를 확인했다(toFloat64 비정확 반올림 → `precise_float_parsing=1`).
- 파일: `raw/<source>/<stream>/dt=YYYY-MM-DD/hour=HH/part-<recv_ts_ms>-<seq>.jsonl.gz`. 조각은 시간 또는 256MB. 닫힌 조각만 재처리 대상. fsync 뒤 rename (정전에서 0B 파일을 남기지 않기, docs/58).
- ClickHouse: `async_insert=1, wait_for_async_insert=1`, 배치 500행 또는 1초 (docs/14 §6: 지연은 배치 창이 정한다 → 저유입 시간대에도 1초 상한).
- 재연결: 무프레임 30초 감지, 즉시 재연결, 백오프 상한 60초 (v1 은 120초 감지·1135초 백오프로 03시 7.7% 유실, docs/58). 재연결마다 `collector_gaps(source, stream, from_ts, to_ts, reason)` 1행.
- 실패 격리: ClickHouse 가 죽어도 파일은 쓴다. 파일 디스크가 죽으면 프로세스를 멈춘다(원본 없는 적재는 하지 않는다).

## 4. ClickHouse 테이블과 멱등 키

| 테이블 | 엔진 | 키 | 파티션 / TTL | 비고 |
|---|---|---|---|---|
| raw_upbit_trades | ReplacingMergeTree(recv_ts) | (market, sequential_id, trade_ts) | 일 / 400일 | docs/13 의 유니크 키 그대로 |
| raw_upbit_orderbook | ReplacingMergeTree(recv_ts) | (market, ts) | 일 / 14일 (파일이 영구 사본) | v1 orderbook_raw |
| raw_binance_trades | ReplacingMergeTree(recv_ts) | (symbol, trade_id) | 일 / 60일 | v1 binance_trades |
| raw_binance_depth | ReplacingMergeTree(recv_ts) | (symbol, last_update_id) | 일 / 14일 | 재구성은 SQL 로 하지 않는다(§8 위험) |
| collector_gaps | MergeTree | (source, stream, from_ts) | - | 재연결 공백 |
| 1분 집계 (upbit·binance orderbook_1m, trades_1m) | dbt incremental, **파티션(일) 교체** | (market, window_start) | 월 / 365일 | v1 MergeTree 중복 문제 제거 |
| marts / quality / dim | dbt, v1 모델 이식 | | | int_reconcile_hourly, dq_* 그대로 |

파생 테이블은 전부 "원본 raw 에서 SQL 로 다시 만들 수 있어야 한다". 그래야 재처리 = 파일 → raw → dbt 한 길이 된다.

## 5. 첫 산출물: rebuild-from-files 와 합격 기준

`scripts/rebuild.py --from <UTC> --to <UTC> [--target <db>]`: 닫힌 파일 조각을 읽어 `<db>.raw_*` 에 넣고(멱등), dbt 를 그 DB 에 돌린 뒤, 라이브 DB 와 마켓×시간 셀 단위로 행 수·cityHash64 합을 대조해 표로 출력한다. 합격 = 전 셀 일치. 이 스크립트가 통과하기 전에는 다른 어떤 기능도 "완료" 라 하지 않는다.

합격선 (docs/59 §7-1):

| 항목 | 기준 | v1 실측 |
|---|---|---|
| 체결 e2e p95 | ≤ 3s | 2.16s 평균 (docs/14) |
| 호가 e2e p95 | ≤ 2s | 1.95s 중앙값 (docs/14) |
| 거래소 대조 | 일 100%, 셀 ≥ 99% | 6일 연속 100% (docs/14) |
| rebuild 하루치 | **측정 뒤 확정** (11-24 실측: 1시간치 246만 행 70초 → 하루 추정 약 28분. 처음 적은 2분은 근거 없는 숫자였다) | 파이썬 파싱 900행/초 → CH 파싱 ~35,000행/초 |
| 메모리 전체 | ≤ 5GB | ≈10GB (docs/58) |
| 정전 복구 | 자동, 사람 0분 | 4시간 (docs/58) |
| 7일 무변경 관찰 | docs/12 가설표 그대로 통과 | |

## 6. 운영

- Airflow(유지, docs/59 §6): `reconcile_trades`·`reconcile_binance`(v1 이식), `dbt_daily`, `compact_raw`(어제 JSONL → Parquet, 검증 뒤 JSONL 삭제), `backup_daily`(Parquet + ClickHouse 백업 → Oracle, dry-run 검증, docs/51·57 의 보존 규칙), `health_check`(v1 16 → 필요한 것만: 수집기 생존·gap·insert 지연·대조·백업 신선도).
- 외부 감시: Oracle watchdog 재가동(cron 주석 해제), heartbeat 는 수집기 생존 + 최근 5분 적재.
- 일일 digest: v1 과 같은 형식(시간대별 p95·호가·알림) → 전/후 비교표.
- 알림 임계: 1주차는 알림 없이 기준선만, 2주차에 분포로 임계 (docs/54).
- 배치 시간: 02~05시 KST (docs/14 §2).

## 7. 일정

| 주 | 산출물 | 검증 |
|---|---|---|
| 0 | 이 문서 확정, docs/60 | 사용자 검토 |
| 1 | 수집기 4개 + 파일 + ClickHouse raw + rebuild + dbt 15모델 + Airflow 7 DAG(compact_raw·backup_daily 를 1주차로 앞당김) | **완료 11-24 01:1x UTC.** rebuild 991셀·898셀 일치, 7일 관찰 11-23 23:53:20 UTC 시작 |
| 2 | Spark 배치(재처리·대조·compact) vs ClickHouse `file()` 측정, Iceberg vs Parquet 폴더 측정, 알림 임계 | 측정표. 이득 없으면 제외 |
| 3 | 7일 관찰 판정, v1/v2 비교 문서, 전원 뽑기 리허설 1회 | 합격선 전 항목 |

## 8. 위험

- Binance 호가 재구성(증분 → 호가장)은 상태가 필요하다. v1 처럼 스트림으로 하지 않고, 필요할 때 Spark/Python 배치로 depth 원본에서 재구성한다. 라이브 호가장 지표가 필요해지면 그때 결정.
- 디스크: **(11-24 실측)** 원본 JSONL.gz ≈ 7.9GB/일(설계 추정의 3배). payload 를 JSON 문자열로 둔 봉투 Parquet(zstd)은 gzip 대비 호가 2.4×·체결 1.1× 뿐이라 ≈ 4~5GB/일 → Oracle 여유 32GB 로 약 6일. 원본 보존 원칙 vs 저장 비용의 결정 필요: (a) 봉투 Parquet 유지 + Oracle 보존 7일, (b) 파싱된 컬럼형 Parquet(v1 14:1) 로 압축하고 봉투 JSONL 은 로컬 N일, (c) 둘 다. 2주차 Spark 비교와 함께 결정.
- ClickHouse async insert 의 지연·중복 특성은 1주차 측정 항목.
- Upbit REST 백필(v1 도구)은 그대로 쓰되 docs/58 §4 의 결함 셋(마켓 목록·404·중단 시 기록) 수정.

## 9. 확장성 - 가능한가, 어디까지가 정직한가

가능하다. 단 "미리 깔아 두는 용량"이 아니라 **"바꿔 끼울 수 있는 계약"** 으로 확보한다. 지금 규모에 맞게 짜되, 다음 네 계약을 지키면 10~100배에서 부품을 교체할 수 있다.

| 계약 | 지금 | 10배(수천/s) | 100배(수만/s, 소비자 여럿) |
|---|---|---|---|
| 메시지 봉투·schema_version | 수집기 → 파일+CH | 그대로 | 수집기 → 로그(Redpanda/Kafka) → 파일+CH. 수집기 코드는 "싱크 하나 추가" |
| 파일 레이아웃(dt/hour 파티션, 닫힌 조각) | 로컬 디스크 | 로컬 + 압축 | 오브젝트 스토리지 경로로 교체(S3 호환). 레이아웃 불변 → Spark·CH 모두 그대로 읽음 |
| raw 멱등 키 | RMT 단일 노드 | 단일 노드(CH 는 수만/s 단일 노드로 처리, docs/23 5,000/s 에서 insert p95 35ms) | Distributed + 샤딩 키 = 지금의 정렬 키 |
| 변환 = SQL (dbt) | dbt on CH | 그대로 | 그대로. 윈도우 집계가 무거워지면 MV 로 내림 |

정직한 한계 둘. 첫째, **1초 미만 실시간 알림이 100배 규모에서 필요하면 스트림 처리기가 다시 필요하다.** 그때는 로그 뒤에 Flink 를 붙이되, 파일이 여전히 원본이고 CH 가 여전히 진실이라는 구조는 유지된다(v1 은 Flink 가 유일한 경로였다). 둘째, **지금 Spark·Iceberg 를 넣어 두는 것은 확장성이 아니다.** 측정 뒤 이득이 있을 때 넣는 것이 확장성이다. 확장을 위해 미리 할 일은 위 표의 계약 네 개를 1주차에 코드와 문서로 고정하는 것뿐이다.

규모가 커지는 순서도 적어 둔다: 유입 수천/s → CH 는 그대로, 수집기 프로세스 분리(마켓 샤드). 소비자가 둘 이상 → 로그 도입. 원본 > 2TB 또는 다른 머신에서 읽어야 함 → 오브젝트 스토리지. 쿼리 동시성 → CH 복제본. 이 순서는 v1 부하 실험(docs/23)과 스택 해체(docs/31)에서 나온 병목 순서와 같다.

## 10. 결정 기록 (2026-11-24, 1주차 구현 뒤 재검토)

원칙: 스택을 고른 것이 아니라 측정이 고르게 한다. 부품마다 "왜 있나"와 "왜 없나"를 숫자로 답한다. 11-24 저녁 "시니어가 주니어 계획을 읽는다"는 관점으로 한 번 걸러 억지 항목 3개를 지우고 증명되지 않은 핵심 1개를 앞으로 당겼다.

| # | 결정 | 왜 (한 문장) | 근거 |
|---|---|---|---|
| 1 | 원문(봉투 JSONL.gz)은 미니PC에 영구 보관. **단일 사본**이며 호스트 소실 시 파싱본(Oracle)으로 복구한다는 위험을 받아들인다 | 재처리 원천은 파서 이전의 원문이어야 한다. v1 은 원천이 Kafka 24h 라 정전 뒤 82분을 못 메웠다 | docs/58 §4, 디스크 396GB / 8GB·일 |
| 2 | 오프사이트(Oracle)엔 **파싱된 컬럼형 Parquet만**, 보존 30일 | 저장은 싸고 읽기가 비싸다. JSON 문자열 Parquet 는 1.07~2.41× 밖에 안 줄고 쿼리마다 전부 읽는다. 컬럼형은 v1 실측 14:1 | compaction_runs, Oracle 여유 32GB |
| 3 | **파싱은 ClickHouse SQL(JSONExtract) 한 곳**. 파이썬 파서는 수집기 테스트용으로 남기되 "같은 봉투 → 같은 행" 동치 테스트로 묶는다 | 파서가 두 곳이면 어긋날 자리가 생긴다. rebuild 900행/초 병목도 파이썬 파싱이다 | rebuild 실측, 하루치 합격선 2분 |
| 4 | **하루치 rebuild 통과를 1주차 완료 조건으로** (3번 뒤 즉시) | "파일에서 전부 다시 만든다"는 v2 의 존재 이유인데 3~6분 창에서만 증명됐다. 하루 규모에서 안 되면 설계가 틀린 것이다 | 미증명 |
| 5 | Kafka·Flink·Debezium 제외 | 284일 측정에서 이 규모(수백~2,000/s)의 세 부품이 한 일은 24h 버퍼와 재처리하면 달라지는 집계였고 비용은 메모리 5GB·정전 복구 4시간 | docs/23·58·60 |
| 6 | ClickHouse + dbt + Airflow 유지 | 값이 맞다는 걸 증명하는 계층이 v1 에서 유일한 "상". 바꿀 이유가 없는 것은 바꾸지 않는다 | docs/59 §1, dbt 테스트 81 |
| 7 | Spark 는 2주차에 **학습 목적으로** 하루치 재파싱 배치 1개를 같은 사양으로 만들고, 이 규모에서 지는 지점을 숫자로 적는다. "공정한 비교"라 부르지 않는다 | 4코어 미니PC 8GB/일에서 결과는 정해져 있다. 포장하면 억지다 | compaction_runs 기준선 |
| 8 | Iceberg 는 **넣지 않는다.** 넣는 조건을 적어 둔다: 엔진 둘 이상이 같은 테이블을 읽고 쓸 때, 오브젝트 스토리지로 옮길 때, 동시 쓰기가 생길 때 | 단일 쓰기·로컬 디스크에선 Parquet 폴더가 90%. 결과를 알면서 하는 측정은 의식이다 | — |
| 9 | 세 엔진(ClickHouse·DuckDB·BigQuery) 스캔 비교는 **하지 않는다.** "컬럼형이 왜 싼가"는 ClickHouse `system.query_log.read_bytes` 로 JSON 문자열 vs 컬럼형을 재면 끝 | 결과로 바꾸는 결정이 없다. Snowflake 는 JD 에 명시된 회사 지원 때만 | — |
| 10 | `schema_version` 변경 시나리오를 한 번 연습한다: 거래소가 필드를 추가했다고 가정 → 버전 올림 → 옛 파일 재처리 | 변경 시나리오 없는 계약은 계약이 아니다 | — |
| 11 | **7일 무변경 관찰 시계는 3·4번 완료 뒤 다시 켠다.** 바꾸는 동안의 관찰은 관찰이 아니다. **(11-25 추가)** 관찰 중 발견한 결함(docs/02 §7)을 고치면 시계를 다시 켠다 — 11-24 02:29 시작분은 22시간 만에 결함 수정으로 종료, 재시작 = 11-25 수정 커밋 시각(1차 00:46, 2차 #4~#6 수정 뒤 03:1x), 판정 12-02 | 11-23 23:53 시작은 무효 | docs/12 가설표 |
| 12 | 합격 = 7일 관찰 + 거래소 대조 100% + 하루치 rebuild + 전원 뽑기 리허설 | "만든 것"과 "도는 것"은 다르다 | docs/14 |

지운 것과 이유: Oracle 봉투 Parquet 3일(복구를 바꾸지 않는 규칙 추가), Iceberg 측정(결과가 뻔함), 세 엔진 스캔 비교(결정에 영향 없음). 바꾼 것: Spark 를 "비교"에서 "학습 + 지는 지점 기록"으로.

남은 일정: 3 → 4 → 11(관찰 7일) → 7 → 10 → 12. 2번은 3번과 같은 작업(SQL 파싱 결과를 Parquet 로 쓰기).
