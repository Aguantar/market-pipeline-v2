# 60. v1 스택 총정리 - 흩어진 기록을 한곳에 (2026-02-12 ~ 2026-11-23)

- 왜 이 문서인가: v1 의 정의·이유·측정·사고가 docs 59편·README·worklog·compose·코드에 흩어져 있다. 포트폴리오에서 "v1 은 어땠고 왜 v2 로 바꿨나"를 말하려면 한 장이 필요하다. 모든 항목은 저장소 파일에서 직접 읽은 사실이고 출처를 붙였다. 못 찾은 것은 "미확인".
- 한 줄 요약: Upbit·Binance 웹소켓 → (MySQL → Debezium) / 직행 → Kafka(3→1 브로커) → Flink(잡 4개) → ClickHouse(79표) → dbt(27모델·81테스트) → Airflow(DAG 11) → Grafana·Slack. 미니PC 1대, 29컨테이너, 284일 운영. 평균 수백 events/s, 피크 2,000/s. 사고 18건, 그중 정전(docs/58)이 종료 결정의 계기.
- 연도 표기: docs/02~05 머리글은 "2025.02.12" 로 적혀 있으나 docs/55 §5 가 "연도 오기로 보이나 확정 못 함"이라 기록했고, 다른 모든 문서와 README 는 2026 기준이다.

## 1. 구성 이력

| 날짜 | 변경 | 출처 |
|---|---|---|
| 2026-02-12 | 최초 구축: MySQL 8.0 + ZooKeeper + Kafka 3브로커(RF3/minISR2, 힙 512MB×3) + kafka-ui. 주식 주문 모의 스키마 | docs/02 §2~4, docs/55 §1-1 |
| 02-12 | Debezium(connect:2.5) MySQL 커넥터, JSON schemas.enable=false | docs/03 §3~4 |
| 02-12 | Flink 1.18(JM 768m, TM 1g·슬롯 2, RocksDB, 60s EXACTLY_ONCE), CdcPipelineJob(5분 집계+이상탐지) | docs/04 §4~5 |
| 02-12~13 | ClickHouse 24.1 + Grafana 11패널, JDBC 싱크 100/5s | docs/05 §3~5 |
| 02-13 | Upbit 실데이터 상시 운영 시작, 초기 5코인 | README 머리글 |
| 02-21~23 | 46시간 적재 중단 → NullSafeStringSchema·restart 전략 | docs/06 §2 |
| 03-11~12 | Airflow 2.8.1(health_check·daily_pipeline), StatsD+Prometheus, Slack 일일 리포트. dbt 3층은 그 이전부터(도입일 미확인) | airflow01~05.md |
| 08-19~30 | 적재 지연 36.9h(유실 아님) | docs/08 |
| 09-06 | 중복 적재 감사(91M행) | docs/07 |
| 09-09 | best bid/ask 4필드, system 로그 15.9→2.4GiB; Flink RocksDB→hashmap, TM 2g·슬롯 4, producer 상한 10→2,500 rows/s; 호가 수집기(Kafka 직행, 6p RF2) + OrderbookJob; 전 코인(287마켓) 확장 | docs/09, 10, 11, worklog 09-09 |
| 09-09~16 | 7일 무변경 관찰 | docs/12, 14 |
| 09-10 | sequential_id 충돌 유실 수정 + 백필 210,469행 | docs/13 |
| 09-16 | reconcile_trades DAG + dbt 계약, 유실 방지 5건 | docs/17, 20 |
| 09-17 | 부하 실험; 백업 도입(네이티브 BACKUP + Oracle 150GB rsync, 복원 79초), 전 사용자 인증; 브로커 3→1; 이상탐지 v2 + dbt quality 층 | docs/23, 21, 24, 22 |
| 09-18 | crypto_trades → ReplacingMergeTree(무정지 12분), Debezium skipped.operations=d | docs/25 §3, 26 §3 |
| 09-19 | 체결 시각 파티션 스왑, 토픽 키=market·zstd·7일, DLQ, 2층 원장(virtual-trader + ledger 커넥터) | docs/28, 29 §7 |
| 09-20 | Binance 체결 493심볼(BinanceTradeJob) + 호가장 재구성 10심볼(BinanceDepthJob), TM 슬롯 8; 하드닝 10건; kafka-ui·Prometheus ops 프로필로 제외; SLO·알럿 3층; 외부 push 감시자(Oracle) | docs/31, 34, 32, 43 |
| 09-24~25 | 16일 조용한 정지 복구, dbt 어댑터 venv | docs/44, 45, 48 |
| 09-26~28 | Binance Trade Flink 잡 ↔ Kafka 엔진+MV 47h 병행 → 컷오버 | docs/50 |
| 10-07~10 | 백업 7일 침묵 → pipefail·주 1회 전체·2세대·Backup Freshness | docs/51 |
| 10-23~26 | Parquet 복원 리허설, 테이블 온보딩 자동화, 컨슈머 랙 감시, 분산 사실표 | docs/52~55 |
| 10-30, 11-04 | 백업 볼륨 100% → free_remote_space, Parquet 30일, 증분 체인 | docs/57 |
| 11-20 | 정전 87분 → 사람 4시간 복구 | docs/58 |
| 11-23 11:19 UTC | compose down. 데이터 마지막 11:15:42 UTC. 11-24 볼륨·로컬 백업 삭제 | docs/59 |

## 2. 컴포넌트별 사실

| 컴포넌트 | 버전 / 이미지 | 메모리 한도 | 실제 역할·설정(실측) | 출처 |
|---|---|---|---|---|
| Kafka | cp-kafka:7.5.3 (README "Kafka 3.5", tech-stack-summary "3.6" - 불일치) | 1280M, 힙 512m | 브로커 1, ZooKeeper 컨트롤러(KRaft 아님), RF1, 토픽 18. 유입 ~270 msg/s = 상한(19k~65k/s)의 0.4~1.4%. "로그로는 일한다, 분산으로는 안 한다" | compose 59~87, docs/55 §1-1, docs/31 §1 |
| 토픽 | | | cdc.crypto_db.crypto_trades 3p zstd 7일 8GB; upbit.orderbook.v1 6p 24h 6GB; binance.trades.v1 6p 3일 8GB; binance.depth.v1 6p 1일; ledger.* 6개 1p 30일; DLQ 3개 | scripts/ops/kafka-topics.sh |
| ZooKeeper | cp-zookeeper:7.5.3 | 384M | 단일 노드, RSS 0.19GiB | compose 39~55, docs/24 §1 |
| Debezium/Connect | connect:2.5 (2.5.4.Final) | 1280M | 커넥터 2: mysql-cdc(crypto_trades, skipped.operations=d, 보관소 모드) / ledger-cdc(원장 6표, 삭제 유지, 거울 모드). 지연 p50 3ms, 태스크 1 | compose 99~128, debezium/*.json, docs/31 §1 |
| MySQL | mysql:8.0 (8.0.45) | 1280M | 체결 원장 7일 보존, 체결 시각 일 파티션, UNIQUE(market, sequential_id), binlog ROW/GTID 30일, 버퍼 풀 512M | compose 2~29, docs/26 §1, docs/28 A |
| Flink | flink:1.18-java11 (1.18.1) | JM 896M / TM 2304M(슬롯 8) | hashmap, 체크포인트 60s EXACTLY_ONCE RETAIN_ON_CANCELLATION, HA 없음, num-retained 1, 재시작 fixedDelay(20, 30s) | compose 132~224, docs/55 §2 |
| Flink 잡 | | | ① CDC Realtime Pipeline(p=2, MarketAlertDetector, 상태 3.65MB) ② Orderbook Pipeline(p=1, 이벤트타임 1분 창, oo 5s/idle 30s) ③ Binance Depth Pipeline(p=2, OrderBookReconstructor 1초 processing-time 타이머) ④ Binance Trade Pipeline(09-20~09-28, MV 로 대체) ⑤ Circuit Connect(타 프로젝트) | docs/55 §2, flink/src |
| Flink 싱크 | | | ClickHouseSinks 배치 200/3,000ms/재시도 3; OrderbookSinks 500/2,000ms | ClickHouseSinks.java, OrderbookSinks.java |
| ClickHouse | clickhouse-server:24.1 (24.1.8.22) | 1792M (768M→1280M→1.75G), 서버 한도 1.57GiB | crypto_trades RMT 월 파티션 TTL 365일 / orderbook_raw MergeTree 일 TTL 7일 / orderbook_1m 365일 / binance_trades RMT 30일 / binance_orderbook_raw 7일·_1m 365일 / candles 60·90일 / 원장 virtual_* RMT / incidents 180일. Kafka 엔진 표 7 + MV 9. 표 79, Replicated 0 | clickhouse/*.sql, docs/48 §5-1, docs/55 §3 |
| Airflow | cdc-airflow:2.8.1, LocalExecutor, PARALLELISM 8 | scheduler 1024M(640M→1G), webserver 640M, postgres 256M | DAG 11: health_check·market_alerts_notify `*/10`, cases_hourly, quality_alerts, reconcile_binance `40 0`, backup_daily `20 1`, rules_daily, reconcile_trades `35 6`, daily_pipeline `0 16`, log_retention(주), weekly_digest(주). 풀 dbt 1·upbit_rest 1 | compose 451~599, airflow/dags |
| health_check | | | 체크 16(clickhouse_ingest, flink_jobs, kafka_health, producer_activity, ingest_lag, market_coverage, kafka_connect, source_busy, parse_failures, ledger_reconcile, mart_freshness, backup_freshness, consumer_lag, binance_ingest, collector_lag, insert_latency). 같은 이름 1시간 억제, CRITICAL 30분 지속 시 승격 | health_check.py |
| dbt | 1.11.6 + dbt-clickhouse 1.10.0 (venv; 09-24 전 1.7.9/1.7.2) | | 모델 27 = staging 2·intermediate 7·dimensions 5·marts 2·signals 2·quality 9. 테스트 81(yml 72 + singular 9), contract 9, 컬럼 설명 314/314 | models/, tests/, docs/45 |
| Grafana | grafana:11.0.0 | 256M | 대시보드 4(프로비저닝 2), readonly_user, Caddy basic_auth | compose 265~297, grafana/ |
| upbit-producer | python:3.11-slim | 256M | Upbit WS 체결 299마켓 → MySQL INSERT IGNORE 배치 100/2s, gap-fill, 상한 2,500 rows/s | producer/, docs/10 §1 |
| orderbook-collector | python:3.11-slim | 256M | Upbit orderbook.15 → Kafka 직행 zstd idempotent key=market, timeout 600s | docs/11 §1, docs/23 §7 |
| binance-collector | 같은 이미지 | 256M | USDT 전 심볼 trade(493→503), 연결당 400 스트림, 23h 선제 재연결, 2분 무프레임 재연결 | docs/31 §4, docs/44 §12 |
| binance-depth-collector | 같은 이미지 MODE=depth | 256M | 상위 10심볼 depth@100ms + 5분 REST 스냅샷 | docs/31 §5 |
| virtual-trader | python:3.11-slim | 256M (프로필 ledger) | Binance Spot Testnet 주문 15분 주기 5심볼 → MySQL 원장 | docs/28 B |
| 알림 | Airflow → Slack 일반·승격 채널 | | n8n 워크플로는 09-17 이후 비활성(JSON 6개 보관) | docs/35 §4, docs/43 §10 |
| 외부 감시자 | Oracle cron + heartbeat-push | | 미니PC 5분 push → Oracle 5분 판정(down 12분). 훈련: 탐지 899초, 거짓 경보 0 | docs/43 §4~8 |
| 백업 | backup_daily DAG | | 네이티브 BACKUP EXCEPT orderbook_raw, 전체 주 1회 2세대 + 증분 체인, Parquet zstd 일 1파일 원격 30일, rsync → Oracle /mnt/backup, verify = dry-run 0 | backup_daily.py, docs/51, 57 |
| 상시 제외(09-20~) | kafka-ui, prometheus v2.50.0, statsd-exporter | 384/256/128M | profiles ops | compose 424~641, docs/31 §7 |

## 3. 데이터 규모

### 3-1. 유입 (events/s)

| 시점 | 값 | 출처 |
|---|---|---|
| 09-09~16 | Upbit 체결 KST 09시 59.4 rows/s 피크, 새벽 11~16; 호가 177~276/s | docs/14 §2 |
| 09-21 평일 14시 KST | 체결 1,591/분 · 호가 14,669/분 · Binance 16,136/분 = 540 rows/s | docs/31 §8-1 |
| 09-26~28 | Binance 체결 평균 441/s, 최대 1,558/s | docs/50 |
| 10-25 24h 평균 | 약 500/s = Binance 200 + Upbit 호가 245 + Upbit 체결 44 + depth 11. 급등 시 2,000 초과 | README |
| 09-23 급등 5분 | 초당 20k, 수집기 한계 ~1만/s | docs/46 §2 |

### 3-2. 저장

| 시점 | 값 | 출처 |
|---|---|---|
| 09-20 | 6.28억 행 / 21.9 GiB / 표 60 | docs/42 §1 |
| 10-25 | 11.6억 행, 32.6 GiB, 표 79. binance_trades 7.76억, crypto_trades 1.84억, orderbook_raw 1.29억. 365일 표 하루 0.3GiB 증가 → 1년 뒤 ~130GiB | README |
| 11-23 최종 | 전체 백업 28.91GiB 79표. Parquet 호가 32일 22GB. 볼륨: clickhouse 49GB·kafka 17GB·mysql 11GB | docs/59 §4, §8 |
| 압축 | orderbook_raw 14.4:1(39.7B/행), crypto_trades 4.4:1, binance_trades 4.5:1 | README, docs/11, 31 |

### 3-3. 보존

MySQL 7일(binlog 30일) / Kafka 체결 7일·호가 24h·Binance 3일 / ClickHouse 365·30·7일 혼합 / 오프사이트 전체 2세대 + 체인 증분 + Parquet 30일(120→75→60→45→30 네 번 축소) - kafka-topics.sh, clickhouse/*.sql, backup_daily.py.

## 4. 측정된 한계·성능

| 항목 | 값 | 출처 |
|---|---|---|
| Kafka 단독 상한(3p RF3 acks=all) | 무배치 19,133/s, zstd 배치 64,956/s | docs/23 §2 |
| 브로커 1대 5분 정지 | ISR 회복 15초, 무손실 | docs/23 §3 |
| 계단 500~2,000/s | 유실 0, CH CPU 21→40% | docs/23 §4 |
| 5,000/s | e2e p95 0.13s, CH 메모리 1.65GiB(한도 1.75) | docs/23 §5 |
| 10,000/s 배치 200 → 1,000 | p95 26.5s → 0.28s, 파트 29,933 → 6,222. 병목 = 동기 JDBC 싱크 + 파티션 3/병렬 2 | docs/23 §5 |
| 전체 브로커 정지 3분 | 체결 무손실(MySQL 앞단), 호가 35,838 유실 → 수집기 timeout 수정 뒤 60초 정지 유실 0 | docs/23 §7 |
| 복원(네이티브) | 79초(1.1억 행, 09-17) / 9분44초(19.67GiB, 10-07) / 3분27초(28.84GiB, 11-23) | docs/21, 52 §4, 59 §4-1 |
| 복원(Parquet) | 21.1M행 63초 444MiB / 17.6M행 114초 401MiB, 전행 해시 일치 | docs/52, 59 |
| e2e 기준선 | 체결 p95 평균 2.16s(최대 3.78s), 호가 p95 1.95s. 지연은 자원이 아니라 배치 창이 결정 | docs/14 §1·§6 |
| 시각 구간 | 거래소→수신 82ms, 수신→MySQL 1,083ms, MySQL→Flink 적재 1,697ms | docs/28 A-8 |
| Flink 상태 | RocksDB 체크포인트 625MB(MANIFEST 352.7MB) → hashmap 17.8KB, CP e2e 1,388ms→51ms | docs/10 §3 |
| 메모리(10-25) | ClickHouse 1,195MB, TM 915MB, Kafka 649MB, MySQL 636MB, 스케줄러 435MB. 전체 ≈10GB → 종료 후 2.9GB | README, docs/59 §5 |
| 만성 | system.metric_log 머지 메모리 한도 실패 매시간(11-19~) | docs/58 §2 |

## 5. 사고

| 날짜 | 사고 / 원인 | 조치 | 출처 |
|---|---|---|---|
| 02-21~23 | 46h 적재 중단: DELETE tombstone → Flink NPE, restart 전략 없음 | NullSafe 파서, restart 전략 | docs/06 |
| 08-19~30 | 지연 36.9h(유실 오판): producer 10 rows/s 상한 포화 | 상한 2,500/s, 체결 시각 대조 | docs/08, 10 |
| 09-09 | n8n mem_limit 크래시 루프 24분; CH TTL 뮤테이션 OOM | 제한 제거; KILL MUTATION | docs/09 |
| 09-10 | 유실 2.94%: 전역 UNIQUE(sequential_id). "업비트 문제" 오판 | UNIQUE(market, sequential_id), 백필 210,469 | docs/13 |
| 09-16 | 대조 분모를 우리 DB 에서 만들어 BFC 6일 무수집 놓침 | 분모=거래소 목록 | docs/17, 18 |
| 09-20 | 스케줄러 OOM 루프 12회; 외부 감시자 8h38m 침묵(pull); Grafana 익명+pipeline 계정 노출 | 1G; push heartbeat; readonly+basic_auth | docs/31 §6, 43, 21 §5 |
| 09-08~24 | 16일 조용한 정지: dbt 테스트·run 실패를 호스트의 다른 dbt 로 "통과" 판정 | 조치 14건 | docs/44, 48 |
| 09-23 | Binance 급등 3.6% 손실(수집기 12.8s 지연 → 거래소 끊음) | collector_lag 체크 | docs/46 |
| 09-30 | Binance 수집기 5h14m "살아 있는 척"(소켓 열림·프레임 0), ~1,100만 건 유실 | 2분 무프레임 재연결 | docs/44 §12 |
| 10-01~07 | 백업 7일 미도착(success 표시): `rsync \| tail` 종료 코드 삼킴 등 | pipefail, Backup Freshness | docs/51 |
| 10-28~30 | 백업 볼륨 100%, 같은 볼륨 Toss Postgres 33h 쓰기 실패 | 사전 정리, 승격, 증분 체인 | docs/57 |
| 11-20 | 정전 87분: CH 0B 파트 크래시 루프, Flink 체크포인트 0B·HA 없음, Kafka 21파티션 절단, Binance 03시 92.26% | 임시 설정·수동 재제출·백필·재계산. 재처리 비결정 잡 2개 발견 | docs/58 |

## 6. 결정과 이유

| 결정 | 이유 | 출처 |
|---|---|---|
| 미니PC 1대 | 비용 0. "가용성이 없는 이유의 전부" | README |
| MySQL 원장을 체결 앞에 | 되돌릴 수 없는 데이터에 원본. 브로커 정지 4회에서 체결을 지킴 | README, docs/26 §2 |
| 호가는 Kafka 직행 | 체결의 14배 건수·48배 바이트, Debezium 봉투 +34% | worklog 09-09 |
| 브로커 3→1 | 같은 호스트·NVMe 라 RF3 내구성은 형식; 비용 2.0GiB·디스크×3 | docs/24 §1 |
| KRaft 미전환 | 데이터 안전성 무관, 되돌릴 수 있는 A안 | docs/24 §3 |
| RocksDB→hashmap | 상태 15KB 인데 MANIFEST 352.7MB 매 체크포인트 복사 | docs/10 §1 |
| committedOffsets(LATEST) | savepoint 없는 재시작에서 latest 점프 유실 방지 | docs/10 §1 |
| 토픽 키=market | trade_id 키는 마켓 내 순서 깨짐(재정렬 5.87~8.09%) | docs/29 §2 |
| ReplacingMergeTree + FINAL | exactly-once 대신 저장 층이 중복 흡수(재전송 27+239건 실측) | docs/25 §1 |
| 체결 시각 파티션 | 프루닝 39/39→5/56, 1.0s→0.10s; MySQL 파티션 DROP | docs/28 A |
| CDC 두 모드(보관소/거울) | 체결 삭제는 보존 정리(토픽 46% 절감), 원장 삭제는 사실 | docs/26 |
| Binance Trade Flink→CH MV | 상태 0인 적재 잡; 47h 병행 불일치 0, 파트 −59% 머지 −72%; 대가 지연 +0.7s | docs/50 |
| "Flink 는 이 규모엔 과했다" | 상태 CDC 3.36MiB·재구성 0.33MiB, 나머지 0 | README, docs/50 §5 |
| 호가 Parquet 아카이브 | 7일 TTL 원본의 유일한 장기본, CH 아니어도 읽힘 | worklog 09-15, docs/52 §4 |
| 백업 제외 기준 | "지금 복구에 쓸 수 있는 유일한 사본인가" | docs/34 §10 |
| 증분 체인 | base=최신 전체면 증분이 전체에 수렴(6.2→12GB) | docs/57 §8 |
| 외부 감시자 push | 죽은 쪽이 대답해야 하는 pull 은 감시자 아님 | docs/43 §4 |
| 알림 억제·승격 | 하루 101~121건 재발송하면 사람이 끔 | docs/48 §4-8 |
| Airflow 유지 | 동적 태스크·REST 풀·백필을 실제로 씀 | README |
| 센서·Celery·K8s 미사용 | 1호스트 16GB 에선 관리 비용만 | portfolio-de-highlights §4 |
| kafka-ui·Prometheus 제외 | 360MB 가 대시보드 1개용 | docs/31 §7 |
| v1 종료·재구축 | 정확성 상, 내구성·자동복구 하. "다듬기보다 fit 하게" | docs/59 §1~2 |

## 7. 자체 평가 (인용)

- docs/42 §7: "약한 곳은 가용성(단일 호스트)과 인증(Kafka SASL) 이고, 둘 다 의도적으로 미룬 선택"
- docs/33 §0: "약한 곳은 ① 보안 노출 ② RMT 를 읽는 모델 6개가 FINAL 없이 ③ 재처리 런북 부재 ④ 시간·명명·코인 키가 관점마다 다름 ⑤ 죽은 산출물 6종 … 금액이 Float64 인 것이 가장 큰 단서"
- README 못 하는 것: 규모 수백 events/s / 분산 처리 경험 없음 / 가용성 없음 / 영구 보존 없음 / Binance 재수집 수단 없음 / Kafka 인증 없음 / 소비자가 저 자신
- README 배운 것: "유실은 지표가 아니라 대조가 찾는다" / "검증은 행 수가 아니라 값으로" / "열려 있는 소켓은 살아 있다는 뜻이 아니다" / "백업은 복원해 보기 전까지 백업이 아니다" / "첫날 숫자를 외삽하지 않는다"
- README 가장 큰 실수: "계약과 테스트 수정을 호스트의 다른 버전 dbt 로 검증하고 '통과'라 적은 것 … 열엿새를 몰랐습니다"
- docs/30 §1: 부재 단정 3회 → "확인 전엔 '아직 못 찾았다'로 말한다"
- docs/58 §0: "정확성 계층은 어디가 비었는지를 10분 안에 알려줬다. 살아남는 능력은 없었다"
- docs/59 §1: 정확성 상 / 관측 상 / 복구 수단 중상 / 내구성·자동복구 하 / 모델 중상 / 규모 하. 새 결함: 재처리 비결정성 2건, 파생 비멱등, fsync 없는 상태 저장소, 수집기 감지 120초·백오프 1135초, 운영 스크립트 부식

## 8. 구성 파일 위치 (저장소 루트 기준)

| 컴포넌트 | 경로 |
|---|---|
| compose | `docker-compose.yml` (mysql 2, zookeeper 39, kafka-1 59, connect 99, flink 132~224, clickhouse 228, grafana 265, 수집기 301~420, kafka-ui 424, airflow 451~599, statsd/prometheus 606~641) |
| MySQL | `mysql/{init.sql, my.cnf, ledger.sql}` |
| Debezium | `debezium/{connector-config, ledger-connector-config}.json` |
| Kafka | `scripts/ops/{kafka-topics, kafka-reduce-to-one, kafka-single-restart-check}.sh`, 계약 `schemas/*.json`(5) + `validate-topic-schemas.py` |
| Flink | `flink/pom.xml`, `flink/src/main/java/com/cdc/pipeline/{CdcPipelineJob, function/MarketAlertDetector, function/CdcEventParser, sink/ClickHouseSinks, orderbook/{OrderbookJob,OrderbookAggregator,OrderbookSinks}, binance/{BinanceDepthJob,OrderBookReconstructor,BinanceTradeJob}}.java`, 테스트 4파일 19 @Test, `scripts/build-flink-job.sh`, `scripts/ops/flink-cdc-redeploy.sh` |
| ClickHouse | `clickhouse/{init,orderbook,binance,ledger,ops,reconcile,pipeline_incidents,market_*,exchange_notices,ingest_repairs,load_test}.sql`, `clickhouse/config.d/{backups,system-logs-ttl}.xml` |
| 수집기 | `producer/producer.py`, `orderbook-collector/collector.py`, `binance-collector/collector.py`, `virtual-trader/trader.py`, `scripts/lib/minws.py` |
| Airflow | `airflow/dags/*.py`(11), `airflow/plugins/`, `airflow/tests/test_dags.py`, `airflow/dbt_profiles/` |
| dbt | `dbt_cdc_pipeline/{dbt_project.yml, profiles.yml, run_dbt.sh, models/{staging,intermediate,dimensions,marts,signals,quality}, tests/*.sql(9), macros/}` |
| Grafana | `grafana/provisioning/dashboards/json/{cdc-pipeline,cdc-anomaly}.json`, 종료 시 export 4개는 Oracle minipc-final/grafana |
| 감시·알림 | `scripts/ops/{cdc-watchdog.py, heartbeat-push.sh, deploy-watchdog.sh, watchdog-drill.sh}`, `n8n/workflows/*.json`(6) |
| 런북 | `scripts/ops/{clickhouse-*-cutover, mysql-trades-partition-swap, reprocess-day, restore-rehearsal, lan-firewall, caddy-grafana-auth}.sh`, `onboard_table.py`, `recompute-orderbook-1m.sql` |
| 문서 | `docs/02~61`, `docs/worklog.md`, `docs/journey-index.md`, `docs/portfolio-de-highlights.md`, `README.md` |
| 종료 시 보존 | Oracle `/mnt/backup/clickhouse/{final_20261123, final_20261123_games, full_20261119, incr_*, parquet}`, `/mnt/backup/minipc-final/`(Grafana JSON·Airflow 메타DB·MySQL DDL/원장·Debezium config·.env·README) |

## 9. 미확인·불일치

- dbt 최초 도입일: 03-11 "현재 dbt 3계층 존재"로만 확인.
- Kafka 버전 표기: compose cp-kafka 7.5.3 ↔ README "Kafka 3.5" ↔ tech-stack-summary "3.6"(초기 문서, 낡음).
- docs/02~05 머리글 연도 "2025" (docs/55 §5 가 오기 가능성 기록).
- docs/59 §2 와 §6 의 Airflow 문장 충돌은 11-24 에 §2 를 고쳐 해소(저녁 결정이 최종).
- docs/58 에 없던 "metric_log 머지 실패 매시간"은 11-24 에 §2 표에 추가(정전 당일 err.log 집계가 근거).
