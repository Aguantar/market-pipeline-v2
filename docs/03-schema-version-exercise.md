# 03. schema_version 변경 연습 (2026-11-24)

docs/01 §10 의 "schema_version 변경 연습 1회" 항목. 봉투 계약에 `schema_version` 을 넣어 두었지만 실제로 바뀌면 어디를 어떤 순서로 고치는지 한 번도 해 본 적이 없었다. 라이브는 7일 관찰 중이라 **코드는 별도 브랜치(`exercise/schema-v2`, worktree `~/market-pipeline-v2-ex`), 데이터는 임시 DB(`mp2_ex_*`)와 파생 파일(`data/exercise/`)** 로만 했다. 실행: `scripts/exercise/schema_v2.py`, 20초, 4개 검증 전부 OK.

## 1. 가상의 변경

Upbit 체결의 `ab`(ASK/BID) 가 `sd`(SELL/BUY) 로 바뀐다고 가정. 실제 11-24 01시 파일 12개(봉투 197,245행, 체결 197,041행)를 읽어 `ab` 를 빼고 `sd` 를 넣은 **schema_version=2 봉투**를 만들었다. 다른 필드·순서·recv_ts 는 그대로. 파서가 모르는 **version=3** 봉투 100행도 만들었다(payload 는 v1 그대로, 숫자만 모름).

## 2. 결과

| 검증 | 실측 | 결과 |
|---|---|---|
| A. 버전을 모르는 현재 MV(main 005)에 v2 파일 | raw 197,041행 전부 `ask_bid = ''`. 행 수는 맞고 값만 빈 문자열 → **조용한 오염**. 셀 대조(행 수+해시)는 잡지만, 거래소 대조(건수)는 못 잡는다 | 예상대로 |
| B. 버전 분기 MV 에 v1 파일 / v2 파일 각각 | 197,041 = 197,041행, 306셀 차이 0, ask_bid 분포 ASK 98,731 / BID 98,310 동일. 각 2.3초 | OK |
| C. v1+v2+v3 를 한 DB 에 | 머지 전 394,082행 → FINAL 197,041행(v1 과 같음, 셀 차이 0). v3 100행은 raw 에 없고 `envelopes_rejected` 에 `(3, 100)` | OK |
| D. MV 교체 리허설 | 파일 3개(28,083 / 24,082 / 18,721 체결) → 옛 MV 만 → 새 MV 를 **다른 이름으로 추가**(둘 다 쓰는 구간 24,082행 중복) → 옛 MV DROP → RENAME → v2 형식 파일. FINAL 70,886 = 합계, `ask_bid=''` 0. 중복은 ReplacingMergeTree 가 흡수 | OK |

## 3. 코드 변경 (브랜치, 라이브 미적용)

`pipeline/sql_parse.py`:
- `KNOWN_VERSIONS[(source, stream)]` - 파서가 아는 버전 목록. 모든 SELECT 의 WHERE 에 `schema_version IN (…)` 게이트가 붙는다(`version_gate`).
- `_by_version({1: expr, 2: expr})` → `multiIf(schema_version = 2, …, <v1 식>)`. 열 하나에 버전별 식을 둔다. upbit/trades `ask_bid` 가 첫 사용.
- `generate_ddl` 이 `envelopes_rejected`(MergeTree, TTL 30일) 와 `mv_envelopes_rejected`(알려진 (source, stream, version) 조합이 아닌 봉투) 를 함께 낸다. **비어 있어야 정상**이고 health_check 가 세어야 한다(관찰 뒤 DAG 수정).
- 테스트 4개 추가(`tests/test_schema_version.py`), 005 재생성. 전체 20 통과.

## 4. 배운 것

1. **Null 엔진 + MV 는 DROP→CREATE 사이의 봉투를 잃는다.** envelopes 는 저장하지 않으므로 MV 가 없는 순간의 insert 는 그냥 사라진다. 그래서 교체 순서는 "새 MV 를 다른 이름으로 추가 → 옛 MV 삭제 → 이름 변경" 이어야 하고, 둘 다 쓰는 구간의 중복은 RMT 자연키가 흡수한다(검증 D). 파일이 1순위라 잃어도 rebuild 로 되살릴 수는 있지만, 안 잃는 순서가 있는데 잃을 이유가 없다.
2. **"모르는 버전은 떨어뜨린다" 가 "조용히 빈 값으로 넣는다" 보다 낫다.** A 가 보여 준 대로 JSONExtract 는 없는 키에 빈 값을 돌려주고 행 수는 정상이다. 게이트 + rejected 표가 있어야 알아챈다.
3. **파서가 두 개면 버전 분기도 두 번이다.** `pipeline/parse.py`(파이썬, 테스트·동치 전용)는 v2 를 모른다. 선택지: (a) 파이썬 파서는 "v1 참조 구현" 으로 동결하고 parity 는 v1 파일만 → 새 버전은 SQL 만 고친다, (b) 파이썬 파서도 같이 고친다(동치 테스트가 v2 도 보장), (c) 파이썬 파서 삭제. 지금은 결정 안 함 - 실제 변경이 올 때 (a) 로 갈 가능성이 높다.
4. **버전을 올리는 기준**: 거래소 원문이 **호환 불가**로 바뀔 때만(키 이름·의미·단위 변경). 키가 추가되면 올리지 않는다(JSONExtract 는 모르는 키를 무시). 버전은 스트림별이다 - 봉투의 `SCHEMA_VERSION` 전역 상수는 실제 변경 때 `(source, stream)` 별 상수로 바꿔야 한다(이번엔 파일을 직접 만들어 수집기 변경은 생략).
5. 1시간치 197k 행 재파싱 2.3초. 버전 분기(multiIf)가 붙어도 속도 차이는 측정 오차 안이었다(v1 2.3s / v2 2.3s).

## 5. 실제로 바뀌면 (rollout 절차)

| 순서 | 할 일 | 검증 |
|---|---|---|
| 0 | 거래소 공지의 변경 시각 T 확인. 바뀐 샘플 메시지 확보 | - |
| 1 | `sql_parse.py`: `KNOWN_VERSIONS` 에 새 버전, 바뀐 열에 `_by_version`. 005 재생성, 단위 테스트 | `pytest`, DDL 파일 = generate_ddl |
| 2 | 임시 DB 에서 **이 연습 스크립트의 B·C** 를 바뀐 샘플로 | 셀 대조 0 차이, rejected 0 |
| 3 | 라이브 MV 교체 - **D 순서**: `CREATE MATERIALIZED VIEW mv_<t>_vN … AS <새 SELECT>` → `DROP TABLE mv_<t>` → `RENAME TABLE mv_<t>_vN TO mv_<t>`. rejected 표·MV 는 그냥 CREATE | 교체 전후 5분 창 raw 건수 연속, `envelopes_rejected` 0 |
| 4 | 수집기: 해당 스트림 `SCHEMA_VERSION` 올려 재배포 (T 이후 메시지는 새 버전으로 봉투에 찍힘) | 재배포 뒤 75초: 4테이블 증가·드롭 0·rejected 0 |
| 5 | T 를 포함한 1시간 창 **rebuild** (파일 → envelopes → 새 MV) 와 라이브 셀 대조 | 전 셀 일치 |
| 6 | 옛 버전 봉투 파일은 그대로 둔다 - 파서가 두 버전을 다 알므로 재처리에 문제 없다 | - |
| 7 | 거래소가 과도기 없이 바꿔 T 이전에 몇 분 새 형식이 옛 버전 번호로 들어왔다면: 그 구간 봉투는 A 처럼 빈 값. 파일에서 `schema_version` 을 고쳐 다시 넣는 게 아니라, **게이트에 `OR (schema_version = 1 AND JSONHas(payload, 'sd'))` 식의 내용 기반 판정**을 임시로 두고 rebuild. (이번 연습 범위 밖, 메모만) | |

## 6. 라이브 반영 시점

관찰 종료(12-01 02:29 UTC) 뒤 `exercise/schema-v2` 를 main 에 합치고 **§5 의 3번 순서로 MV 4개 + rejected 표를 라이브에 적용**한다. 그 자체가 이 절차의 두 번째 리허설이다. 합치면 health_check 에 `envelopes_rejected` 건수 조건 추가. 관찰 중에는 브랜치만 둔다(worktree 는 라이브 마운트와 무관 - 컨테이너는 `~/market-pipeline-v2` 의 main 을 본다).
