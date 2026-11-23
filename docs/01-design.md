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
- 메시지 봉투: `{source, stream, schema_version, recv_ts, payload}`. payload 는 거래소 원문 그대로. 파서는 수집기에 없다 → 수집기는 "받아서 두 곳에 쓴다" 만 한다.
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
| rebuild 하루치 | ≤ 2분 / ≤ 500MiB | file() 63~114초·401~444MiB (docs/52·59) |
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
| 1 | 수집기 3개 + 파일 + ClickHouse raw + rebuild.py + dbt 이식 + Airflow 4 DAG | rebuild 전 셀 일치, 7일 관찰 시작 |
| 2 | Spark 배치(재처리·대조·compact) vs ClickHouse `file()` 측정, Iceberg vs Parquet 폴더 측정, 알림 임계 | 측정표. 이득 없으면 제외 |
| 3 | 7일 관찰 판정, v1/v2 비교 문서, 전원 뽑기 리허설 1회 | 합격선 전 항목 |

## 8. 위험

- Binance 호가 재구성(증분 → 호가장)은 상태가 필요하다. v1 처럼 스트림으로 하지 않고, 필요할 때 Spark/Python 배치로 depth 원본에서 재구성한다. 라이브 호가장 지표가 필요해지면 그때 결정.
- 디스크: 원본 JSONL.gz 추정 2~3GB/일(측정 전). compact 뒤 Parquet ~1GB/일. 466GB 에서 raw 1년 가능. 측정 뒤 보존 기간 확정.
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
