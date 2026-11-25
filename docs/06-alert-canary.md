# 06. 알림 경로 카나리 (2026-11-25)

## 문제
11-24 05:10~06:10 UTC, health_check 가 critical 7건을 감지했지만 `send_health_alert` 의 KeyError 로 **한 건도 Slack 에 가지 않았다**(docs/02 §7 #1·#8). 알림을 보내는 코드가 죽으면 "알림이 없다" 와 "문제가 없다" 가 구분되지 않는다. 감시자(Oracle)는 heartbeat 부재·ClickHouse·적재 정지만 봤고 **알림 경로 자체**는 보지 않았다.

## 설계 — 밖에서 세는 카나리
```
Airflow alert_canary (07:10 UTC) ─ send_health_alert(같은 경로) ─▶ Slack  "alert-path ok"
        │ 2xx 일 때만
        ▼
/data/logs/alert_canary.json {sent_at, epoch}
        │ heartbeat-push.sh (5분) 가 alert_canary_at/epoch 로 실어 보냄
        ▼
Oracle cdc-watchdog: alert_canary_epoch 가 없거나 26시간 넘게 갱신 안 됨 → degraded → Slack(감시자 자체 웹훅, Airflow 와 독립)
```
- 카나리는 **진짜 경로**를 지난다: `send_health_alert` → `_send_slack_message` → webhook. 이 함수들이 11-25 부터 전달 여부(2xx)를 `bool` 로 돌려준다. 2xx 가 아니면 마커를 안 쓰고 태스크가 실패한다.
- 감시자는 마커의 **나이**만 본다. Airflow 가 죽어도, DAG 가 깨져도, Slack 변수가 사라져도, 코드가 KeyError 를 내도 결과는 같다: 26시간 뒤 "카나리 갱신 없음".
- 한계 26시간 = 하루 1회 + 여유 2시간. 하루에 한 번 울리는 메시지는 "알림이 살아 있다" 는 증거이자 소음의 상한이다.

## 11-25 실측
| 항목 | 값 |
|---|---|
| 카나리 수동 1회 | 13:56:30 UTC 발송, Slack 2xx, 마커 기록. DAG 가 생성 직후 paused 라 4분 대기 → unpause |
| heartbeat 에 실림 | 13:56:32 UTC 첫 push 에 `alert_canary_at/epoch` 포함 |
| 감시자 배포 | 13:56:12 UTC, self-test **13/13 통과**(카나리 신선 ok / 27시간 침묵 degraded / 필드 없음 degraded 추가) |
| 감지 지연 상한 | 26시간 + 감시자 cron 5분 |

## 바뀐 파일
- v2: `airflow/dags/alert_canary.py`(신규), `airflow/plugins/callbacks/slack_callbacks.py`(bool 반환), `scripts/ops/heartbeat-push.sh`(필드 2개)
- v1 저장소: `scripts/ops/cdc-watchdog.py`(CANARY_SEC, 판정, self-test 3건) → Oracle `~/cdc-watchdog/` 배포, 이전본 `.bak.20261125`

## 안 한 것
- Slack 쪽 장애(웹훅 폐기·채널 삭제)는 2xx 가 안 오므로 잡힌다. Slack 이 2xx 를 주고 메시지를 버리는 경우는 못 잡는다 — 그건 Slack 의 문제.
- 카나리를 critical 채널(`slack_webhook_critical`)로도 보내는 것: 하루 1건이라도 휴대폰 알림 채널에 소음을 넣지 않기로. 승격 경로는 실제 critical 때 한 번 더 확인하는 수밖에 없다.
