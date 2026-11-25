# 02. v1 ↔ v2 전·후 비교

작성 2026-11-24. v1 열은 전부 채웠고(출처 표기), v2 열은 7일 관찰 판정일(**2026-12-02 03:10:23 UTC, 11-25 마지막 수정 커밋 + 7일**; 원래 12-01 02:29 였으나 §7 의 수정으로 재시작)에 채운다. v2 "첫날" 열은 11-24 03:02 UTC 한 스냅샷이며 참고값이다.

같은 단위로 재기 위해 v2 에도 5분 관측기를 뒀다: `scripts/observe/collect_metrics.sh`(호스트 cron 5분) → `mp2.ops_metrics_5m` + `~/pipeline-observation/mp2_metrics_5m.csv`. v1 은 `~/pipeline-observation/metrics_5m.csv`(21,663행, 2026-09-09 04:58 ~ 11-23 11:15 UTC, 75일). 관측기는 읽기만 하며 파이프라인 경로를 바꾸지 않는다(관찰 시계 유지).

## 0. 비교 전에 알아야 할 단위 차이

| 항목 | v1 측정 | v2 측정 | 비교 가능? |
|---|---|---|---|
| 체결 "지연" | `now() - trade_ts` 를 ClickHouse 에 **적재된 행** 기준으로 5분마다 (이벤트 → 조회 가능, Debezium·Kafka·Flink·JDBC 배치 포함) | `recv_ts - trade_ts`(이벤트 → 수집기 수신)와 `freshness`(스냅샷 시각 − 표의 max(recv_ts), 적재 신선도) 둘 | 직접 비교는 **v1 e2e ↔ v2 lag + freshness** 합으로. v2 는 수집기 → CH 사이에 Kafka/Flink 가 없어 배처(500행/1초) 만 끼어 있다 |
| 호가 e2e | 수집기 → Kafka → Flink → CH 적재 후 `now() - ts` | 같은 방식의 lag + freshness | 같음 |
| 음수 lag | 없음(적재 시각 기준) | 호가 −2ms·depth −12ms 가 나온다 = 거래소 서버 시각이 우리 수신 시각보다 늦음(시계 오차). 결함이 아니라 측정 한계. 체결은 거래소가 체결 시각을 찍고 전파하므로 +46ms | 비교할 때 "지연 ≈ 0" 으로 읽는다 |
| 5분 행수 | Upbit 체결·호가만(Binance 는 별도 열 없음) | 4 스트림 | Upbit 두 스트림만 비교 |
| 호가 행 정의 | 스냅샷 1행(15호가 배열 → best 4필드 + 배열) | 스냅샷 1행(15호가 배열 전체를 Decimal 배열로) | 행 수는 비교 가능. **v2 는 300마켓 전부 orderbook.15**, v1 은 09-09 이후 287~300마켓 → 같은 수준 |
| 메모리 | 컨테이너 RSS 합(docker stats), ClickHouse 는 `MemoryTracking` | docker stats RSS | 같음 |

## 1. 지연·처리량

| 지표 | v1 (75일 5분창 분포) | v1 (관찰 주간 docs/14) | v2 첫날 스냅샷 (11-24 03:02 UTC) | v2 7일 (12-02 채움) | 합격선 (docs/01 §5) |
|---|---|---|---|---|---|
| Upbit 체결 e2e p50 | 중앙값 **1.11s**, p95 1.23s | - | lag p50 **46ms** + freshness 573ms | | - |
| Upbit 체결 e2e p95 | 중앙값 **2.20s**, p95 2.27s, max 532s(백필 창) | 평균 2.16s, max 3.78s | lag p95 **113ms** | | ≤ 3s |
| Upbit 호가 e2e p50 | 중앙값 **856ms**, p95 1,054ms | - | lag p50 −2ms, freshness 72ms | | - |
| Upbit 호가 e2e p95 | 중앙값 **1,925ms**, p95 1,983ms. 월별 중앙값 09월 1,936 / 10월 1,951 / 11월 1,942 (완전히 평평 = 배치 창이 결정) | 중앙값 1.95s, max 2.01s | lag p95 **11ms** | | ≤ 2s |
| Upbit 체결 5분 행수 | 중앙값 **5,590**(18.6/s), p95 16,329, max 212,254(백필) | 피크 KST 09시 59.4/s | **9,453**(31.5/s, 221마켓) | | - |
| Upbit 호가 5분 행수 | 중앙값 **63,975**(213/s), p95 111,409, max 328,493 | 177~276/s | **92,434**(308/s, 300마켓) | | - |
| Binance 체결 5분 행수 | (열 없음) 09-21 14시 KST 16,136/분 = 269/s | | **88,990**(297/s, 469심볼) | | - |
| Binance depth 5분 행수 | (열 없음) 정상 ~49,000/82분 ≈ 50/s (docs/58) | | **24,951**(83/s, 10심볼) | | - |
| 마켓 수 | 체결 중앙값 204 / 호가 294(max 302) | 287~299 | 체결 221 / 호가 300 / Binance 469 / depth 10 | | - |

v1 의 "지연 2초" 는 자원 부족이 아니라 producer 2초 배치 + Flink JDBC 싱크 2~3초 배치 창의 합이었다(docs/14 §6, docs/23 §4). v2 는 그 두 단을 없앴으니 e2e 가 1초 아래로 내려오는 것은 **설계상 당연한 결과**이고, 비교의 의미는 "7일 동안 그 수준이 유지되는가·꼬리(p95·max)가 어디까지 튀는가" 에 있다.

## 2. 자원

| 지표 | v1 | v2 첫날 | v2 7일 | 합격선 |
|---|---|---|---|---|
| 컨테이너 수 | **26**(docs/23 §1) | **8**(clickhouse, 수집기 4, airflow 3) | | - |
| 호스트 used(15GB 중) | ≈10GB(10-25 README), 종료 후 2.9GB(docs/59 §5) | **4.0GB**(관측 포함, 다른 서비스 공존) | | ≤ 5GB |
| ClickHouse RSS | 75일 중앙값 **0.86GiB**, p95 1.05, max 2.56; 한도 1.75G(768M→1280M→1.75G 세 번 상향) | **1.08GB**(한도 3G) | | - |
| 수집기 RSS | producer·orderbook·binance·depth 각 256M 한도, 실측 미기록 | 27 / 48 / 107 / 34 MB | | - |
| Kafka+ZK+Connect+Flink | ≈ 649 + 190 + ~900 + 915(TM) MB (docs/60) | **0** | | - |
| 이미지 | kafka·zk·connect·flink·mysql·clickhouse·airflow·grafana (수 GB) | mp2-collector 241MB, mp2-airflow 2.17GB, clickhouse 1.29GB, postgres 392MB | | - |
| 메모리 한도 상향 사건 | ClickHouse 3회, Flink TM 1→2g | | | 0 |

## 3. 저장

| 지표 | v1 | v2 (11-24 실측) | v2 7일 | 비고 |
|---|---|---|---|---|
| 원본 보존 형태 | Kafka 로그(체결 7일·호가 24h·binance 3일) + ClickHouse(orderbook_raw 7일 TTL) — **원본은 보존 안 됨**, 재처리 = Kafka 재소비 | 봉투 JSONL.gz(불변 파일) + 컬럼형 Parquet 일 1회 → Oracle 14일 | | v2 원칙 §1-1 |
| 원본 일 증가 | Kafka 브로커1 +0.4~0.7GB/일, 플래토 15GB (docs/14) | JSONL.gz **≈7.9GB/일**(호가 913MB/3h 가 86%) | | Oracle 32GB 여유 → 컬럼형 Parquet 로 오프사이트 |
| 압축률 (CH, 압축 전/후) | orderbook_raw **14.4:1**, crypto_trades 4.4:1, binance_trades 4.5:1 | raw_upbit_orderbook **14.45:1**, raw_upbit_trades 4.3:1, raw_binance_trades 5.06:1, raw_binance_depth 2.44:1 | | 같은 열 구성이면 같은 압축률 — 엔진이 아니라 데이터가 정한다 |
| 봉투 Parquet vs gzip JSONL | - | 호가 2.41×, 체결 1.07~1.12×, depth 1.02× (payload 가 문자열이라) | | §10 #4 결정 근거 |
| 컬럼형 Parquet 일 크기 | 호가 Parquet 일 1파일 ~700MB(backup_daily) | 11-25 00:20 첫 export 뒤 채움 | | |
| 백업 검증 | rsync dry-run 0 + Backup Freshness(docs/51) | rsync dry-run 0 + SHA256 + export 때 라이브 셀 대조 | | |
| 복원(하루치) | 네이티브 BACKUP 복원 3분27초(28.84GiB) / Parquet 21.1M행 63초 | rebuild 1시간 246만 행 **70초** → 하루 추정 ~28분(미실측) | | 합격선 "측정 뒤 확정" |

## 4. 정확성·내구성·복구

| 지표 | v1 | v2 첫날 | v2 7일 | 합격선 |
|---|---|---|---|---|
| 거래소 대조(시간 캔들 vs 적재) | 관찰 주간 6일 연속 100%(docs/14), 평시 Binance 99.97% | 11-25 00:40(Binance)·06:35(Upbit) 첫 자동 실행 | | 일 100%, 셀 ≥ 99% |
| 파일 ↔ 라이브 셀 대조 | 해당 없음(원본 파일 없음) | 1시간 창 2,248셀 일치(11-24 02:3x), 6분 창 991셀·898셀 일치 | 매일 00:20 export_columnar 가 하루치 대조 | 전 셀 일치 |
| 재처리 결정성 | **비결정 잡 2개**(OrderBookReconstructor processing-time 타이머, Upbit 1분 창 watermark skew — docs/58 §5) | 파싱은 SQL 한 곳, 집계는 `insert_overwrite` 일 파티션 → 두 번 돌려도 같은 결과(dbt 1차에서 확인) | | - |
| 중복 처리 | ReplacingMergeTree + FINAL (sequential_id 충돌 유실 1회, docs/13) | ReplacingMergeTree(자연키) + FINAL 은 staging 에서만 | | - |
| 수집기 재연결 | 관찰 주간 2회/주(서버측 close) | `collector_gaps` 1시간 1건(호가, 03:02 시점) | | - |
| 정전 복구 | **사람 4시간**: CH 0B 파트 크래시 루프 → 임시 설정, Flink HA 없음 → 수동 재제출 4잡, Kafka 절단 21파티션, REST 백필 195,657행, SQL 재계산 23,574행 (docs/58) | 미실험 — compose restart + 불변 파일(fsync·rename) + MV 가 설계상 답. **전원 뽑기 리허설은 관찰 종료 뒤** | | 자동, 사람 0분 |
| 조용한 정지 | 16일(docs/44), 백업 7일 침묵(docs/51) | health_check 10분 + Oracle 감시자 heartbeat 5분(세 스트림 모두 0 일 때 stale) | | 7일 무사고 |

## 5. 운영 복잡도

| 지표 | v1 | v2 |
|---|---|---|
| 데이터 경로 단 수 (체결) | WS → producer → MySQL → Debezium → Kafka → Flink → CH (6단) | WS → 수집기 → 파일 + CH envelopes → MV (2단) |
| 상태를 가진 컴포넌트 | MySQL, Kafka, ZK, Flink 체크포인트, CH, Airflow PG | CH, Airflow PG (+ 파일) |
| 알아야 할 설정 면 | Kafka 보존·ISR·오프셋 커밋, Flink 체크포인트·watermark·idleness, Debezium 스냅샷 모드, JDBC 배치 | 봉투 형식, MV SELECT, 배처 500/1s, dbt 파티션 |
| DAG 수 | 11 | 7 |
| dbt 모델 | 다수(v1 README) | 15 + 테스트 6 |
| 문서 | docs 01~60 | 00~02 + worklog |

## 6. 12-02 에 채울 것 (절차)

1. `mp2.ops_metrics_5m` 에서 **11-25 2026-11-25 03:10:23 ~ 12-02 2026-11-25 03:10:23 UTC**(재시작 창) 구간으로 §1·§2 의 v2 열 계산(중앙값·p95·max). 쿼리는 아래.
2. `compaction_runs`·`backup_daily` 로그로 §3 컬럼형 Parquet 일 크기·export 소요 평균.
3. `dq_*_reconcile_daily` 7일분으로 §4 대조율.
4. `pipeline_incidents`·`collector_gaps` 7일 합계.
5. 미달 항목은 숫자 그대로 적고 원인은 별도 절. **합격선 맞추려고 측정 창을 바꾸지 않는다.**

```sql
SELECT stream,
       quantile(0.5)(lag_p95_ms) lag_p95_median, quantile(0.95)(lag_p95_ms) lag_p95_p95, max(lag_max_ms) lag_max,
       quantile(0.5)(freshness_ms) fresh_median, quantile(0.95)(freshness_ms) fresh_p95,
       quantile(0.5)(rows_5m) rows_median, quantile(0.95)(rows_5m) rows_p95,
       max(container_mem_mb) mem_max, sum(gaps_1h > 0) snapshots_with_gap
FROM mp2.ops_metrics_5m
WHERE ts BETWEEN '2026-11-25 03:10:23' AND '2026-12-02 03:10:23' AND stream != 'host'
GROUP BY stream ORDER BY stream;

SELECT quantile(0.5)(ch_mem_mb), quantile(0.95)(ch_mem_mb), max(ch_mem_mb), max(host_used_mb), min(disk_free_gb), max(raw_dir_gb), max(parquet_dir_gb)
FROM mp2.ops_metrics_5m WHERE stream = 'host' AND ts BETWEEN '2026-11-25 03:10:23' AND '2026-12-02 03:10:23';
```

## 7. 관찰 중 발견 (11-24 02:29 ~ 11-25 00:43 UTC, 첫 22시간)

"7일 무변경" 은 첫날에 깨졌고, 그게 관찰의 목적이었다. 발견 6건, 전부 수정(11-25 00:40~01:1x 배포). 원본 파일은 전부 무사했고, 하루 고유 키 2,259,064 = ClickHouse FINAL 2,259,064 로 **손실 0** 이 확인됐다.

| # | 발견 | 영향 | 잡은 수단 | 조치 |
|---|---|---|---|---|
| 1 | health_check → Slack 경로 `KeyError('name')` (v1 플러그인 계약 불일치) | 05:10~06:10 critical 7건 미발송, DAG 7회 실패 | 11-25 점검에서 DAG 실패 이력 | **수정**: 키 매핑, 테스트 발송 HTTP 200 |
| 2 | Upbit 체결 `"bbp":null` 1건 → `toDecimal128('null')` 예외 → 500행 배치 거부 → 큐 머리 차단 | CH 적재 **79분 정지**(05:05~06:24), 1,500행 드롭(파일엔 있음) → 00:55 파일 재투입으로 복구. "04시 273행 결손" 은 파일 중복(재접속 재전송)으로 판명, 손실 아님 | health_check critical(incidents), 수집기 STATS, 파일 vs CH 셀 대조 | **수정**: MV null 가드(0.34초 무중단 교체) + 배처 독 행 격리(이분 → 단건 격리) |
| 3 | Binance depth 심볼 목록을 순서까지 비교 → 순위만 바뀌어도 재접속 | 시간당 6~10초 공백 ×4회/일 | collector_gaps + 로그 `+[] -[]` | **수정**: 집합 비교 |
| 4 | export 검증이 Parquet 를 되읽을 때 배열을 `Array(Nullable)` 로 추론 → cityHash64 불일치 | depth 242/242·orderbook 7,219 셀 거짓 DIFF, 데이터는 행수·해시(CAST 후) 일치 | 첫 자동 export_columnar(11-25 00:20) | **수정**: `schema_inference_make_columns_nullable=0` |
| 5 | export INSERT 실패 뒤 부분 Parquet 가 남고 backup 이 그대로 복사 | 부분 파일 4개(2.3GB)가 백업 대상 디렉터리에 | #2·#4 조사 중 | **수정**: `.tmp/` 에 쓰고 검증 통과 뒤 rename, 실패 시 삭제. 4개는 격리 |
| 6 | 거래소가 재접속 뒤 같은 체결을 다시 보내 파일에 중복(하루 2,290행) → export Parquet 셀 행수 > 라이브 FINAL | 재접속 있는 날은 export 가 거짓 DIFF 로 실패 | 재투입 뒤 파일 vs CH 고유 키 대조 | **수정**: 출력은 파싱한 그대로 두고 **검증을 라이브와 같은 의미**로 — 키별 마지막 수신본(`argMax(hash, recv_ts)`)만 센다(호가는 같은 키로 내용이 다른 스냅샷도 와서 단순 중복 제거로는 부족했다). 출력에서 중복 제거(LIMIT BY·GROUP BY)는 호가 23M 행에서 메모리 한도를 세 번 넘어 포기 |

교훈: ① "파일 1순위" 는 손실을 막았지만 **CH 적재 경로의 독 행 한 건이 스트림 전체를 멈추는 것**은 설계가 아니라 측정이 알려 줬다. ② 알림 경로는 **실제로 한 번 울려 봐야** 믿을 수 있다(v1 코드 재사용이 계약 불일치를 숨겼다). ③ 검증 코드도 틀릴 수 있다 — 거짓 DIFF 를 "데이터 손실" 로 읽지 않으려면 행수와 해시를 따로 본다.

## 출처

- v1 5분 관측: `~/pipeline-observation/metrics_5m.csv`(v1 `scripts/observe/collect_metrics.sh`), 통계는 2026-11-24 계산. 체결 lag 열은 600초, 호가 e2e 는 60초 초과 값(백필·재시작 창)을 뺀 분포. `c_reconnects` 열은 누적 카운터가 리셋·오염되어 75일 합계는 쓰지 않았다(관찰 주간 docs/14 값 사용).
- v1 문서: `cdc-realtime-pipeline/docs/14`(관찰 주간), `23`(부하 실험), `58`(정전 복구), `59`(종료), `60`(스택 fact sheet).
- v2: `mp2.ops_metrics_5m`, `system.parts`, `docker stats`, `du`, `docs/worklog.md` 11-24 항목.
