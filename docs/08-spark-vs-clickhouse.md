# 08. Spark 하루치 재파싱 1회 — 학습과 지는 지점 (2026-11-25 실행, 11-27 기록)

docs/01 §10 #7: "Spark 는 학습 목적으로 하루치 재파싱 배치 1개를 같은 사양으로 만들고, 이 규모에서 지는 지점을 숫자로 적는다. 공정한 비교라 부르지 않는다."

## 같은 입력·출력·검증
- 입력: 11-24 봉투 JSONL.gz 4스트림 1,156 조각(7.1GB). 출력: 스트림당 컬럼형 Parquet 1파일(zstd). 검증: ClickHouse 가 Parquet 를 되읽어 라이브 raw FINAL 과 (cell, hour) 행수·cityHash64 대조 — compact.py 와 같은 쿼리(키별 마지막 수신본).
- Spark 4.3.0, Java 17, `local[2]`, driver 2g, `spark.read.text` → `from_json`(payload 는 문자열) → `get_json_object`/`from_json` 배열 → `timestamp_millis`, `CAST DECIMAL(20,8)` → `coalesce(1).write.parquet`. 코드 `spark/export_day_spark.py`(파서는 `sql_parse.SELECTS` 의 Spark SQL 판).
- ClickHouse 쪽 기준선은 11-25 02:53~03:08 의 export_columnar 4차(같은 날, 같은 4코어 호스트, `max_threads=2`).

## 결과 (ds=2026-11-24)

| 스트림 | 행 | ClickHouse 초 | Spark 초 | 배수 | Parquet 크기 CH → Spark |
|---|---|---|---|---|---|
| upbit/trades | 2,259,064 | 20 | 143 | 7.2× | 67.8 → 50.8 MiB (−25%) |
| upbit/orderbook | 23,488,337 | 637 | **3,811** | 6.0× | 1,267 → 1,108 MiB (−13%) |
| binance/trades | 20,397,777 | 61 | 740 | 12.1× | 230 → 152 MiB (−34%) |
| binance/depth | 6,789,081 | 93 | 581 | 6.2× | 727 → 604 MiB (−17%) |
| **합** | 52,934,259 | **811 (13.5분)** | **5,275 (87.9분)** | **6.5×** | 2,292 → 1,914 MiB (−16%) |

- 검증: **4/4 전 셀 일치**(7,045 / 7,219 / 12,611 / 242셀). 두 파서(ClickHouse JSONExtract, Spark get_json_object)가 같은 봉투에서 같은 행을 냈다 — 봉투 계약이 엔진에 묶여 있지 않다는 증거.
- Spark 시간은 11-25 17:00~18:28 UTC 로그의 스트림 시작 시각 차이(각 스트림 = 읽기+파싱+쓰기, 검증 제외). 11-25 첫 실행은 **제 검증 SQL 의 따옴표 중첩 버그**(`CAST(x, 'DateTime64(3, 'UTC')')`)로 기록이 안 돼, 11-27 에 `--verify-only` 로 같은 Parquet 를 재검증해 기록했다(검증만 3.8 / 70.8 / 34.3 / 19.0초).

## 어디서 지나 (이 규모·이 호스트에서)
1. **gzip 은 쪼갤 수 없다.** 조각 1개 = 태스크 1개(스트림당 289개), `local[2]` 라 2개씩. ClickHouse 도 같은 파일을 읽지만 파싱이 벡터화 C++ 이고 프로세스 간 전달이 없다.
2. **JSON 파싱 경로.** Spark 는 행마다 `from_json`(봉투) → `get_json_object`(필드별 재파싱) 2단. 호가는 배열까지 `from_json` 3단. ClickHouse 는 `JSONExtract*` 가 한 번 훑는다. 호가에서 6배, 체결에서 12배 차이가 난 이유의 대부분.
3. **`coalesce(1)` 쓰기.** 파일 1개로 모으려고 쓰기를 1코어로 묶었다. 파일을 여러 개로 두면 쓰기는 병렬이 되지만 다운스트림(rebuild `list_chunks`, backup) 모양이 바뀐다 — 이 규모에선 1파일이 운영상 낫다.
4. **JVM 기동 6초 + 세션 1개로 4스트림 직렬.** 하루 1회 배치라 상수항은 작지만, 1시간치 같은 작은 작업엔 비율이 커진다.

## 어디서 이기나
- **Parquet 가 13~34% 작다.** Spark 의 Parquet writer 는 딕셔너리 인코딩·row group 기본값이 다르고 zstd 를 같은 레벨로 써도 더 작게 나왔다. 오프사이트(Oracle 블록 32GB·Object Storage 20GB)에선 의미 있는 차이. ClickHouse 쪽 `output_format_parquet_*` 설정으로 좁힐 수 있는지는 다음 측정 대상.
- 표현력: 파이썬 UDF·ML·조인이 필요한 재처리(예: Binance 호가장 재구성, docs/01 §8)는 Spark 쪽이 자연스럽다. 지금은 그런 작업이 없다.

## 결론 (§10 #7 에 돌려주는 답)
이 규모(하루 5,300만 행·7GB)에선 ClickHouse `file()` 재파싱이 6.5배 빠르고 운영 부품이 0개 더 적다. Spark 를 넣을 조건은 §9 와 같다: 노드가 2대 이상이거나, 파싱이 아니라 **상태 있는 재구성**이 필요해질 때. 그 전까지 Spark 코드는 저장소에 두고(학습·계약 검증용) 스케줄에는 올리지 않는다.
