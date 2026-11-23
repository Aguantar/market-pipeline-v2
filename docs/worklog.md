# worklog (v2)

- 11-23 23:2x~23:5x UTC **1주차 시작.** 저장소 `~/market-pipeline-v2`(git, uv venv). 봉투·불변 파일 조각(시간/5분/256MB 회전, Z_SYNC_FLUSH+fsync 5초, .tmp 복구)·raw 스키마 4표(RMT 자연키, Decimal)·파서·CH 배처(500행/1초, 실패 재시도·상한 drop) + 테스트 16개 통과.
- 11-23 23:34 UTC 사고: `cd &&` 미적용으로 v2 파일 3개가 v1 저장소에 덮어써짐(.env 포함) → git/Oracle 보존본으로 복구, 데이터 손실 없음. 이후 절대경로만 사용. (v1 worklog 에도 기록)
- 11-23 23:42~23:46 UTC 수집기 4개 호스트 스모크 90초: 수신=파싱=CH, 파일 키=CH 키, 드롭 0. upbit trades 1,970 / binance trades 12,707 / upbit orderbook 23,091(300마켓) / binance depth 7,093(10심볼, 스냅샷 20).
- 11-23 23:47 UTC **상시 수집 시작** (compose, uid 1000, restart unless-stopped). 설계 §1-4 를 systemd → compose restart 로 변경(이유 문서화).
- 11-23 23:5x UTC **v2 사고 #1 (rebuild 도구가 첫 실행에서 발견)**: `recover_tmp()` 가 데이터 루트 전체의 .tmp 를 복구 대상으로 삼아, 23:49 재기동 때 각 수집기가 다른 수집기의 열린 .tmp 를 지움 → upbit trades/orderbook·binance depth 프로세스가 지워진 inode 에 계속 써서 23:47~00:0x 파일 유실(CH 에는 있음). binance trades 만 마지막에 떠서 무사. 수정: 복구를 (source, stream) 자기 디렉터리로 한정 + 회귀 테스트. 교훈: "파일이 1순위" 는 코드가 아니라 **대조가 지키는 것**이다 — rebuild 가 없었으면 조용히 지나갔다. 첫 대조에서 잡힌 또 하나: 창 판정은 이벤트 시각으로(접속 시 Upbit 가 마켓별 마지막 체결·호가 스냅샷을 보내 recv 와 event 가 어긋남) → `--late-slack-sec`.
