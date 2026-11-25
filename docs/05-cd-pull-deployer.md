# 05. CD — pull 방식 배포기 `mp2-cd` (2026-11-25)

## 왜 pull 인가
미니PC 는 공개 IP 가 없고 인바운드를 열지 않는다(SSH 도 WireGuard 안으로 옮기는 중, docs/59 §6 하드닝). GitHub 가 미니PC 로 밀어 넣는(push) 길이 없으니 미니PC 가 5분마다 레지스트리를 보고 끌어온다(pull). GitOps 계열 도구(Argo·Flux·Watchtower)와 같은 방향인데, 컨테이너 4개짜리에 그 도구를 올리는 것은 docs/01 §9 의 "측정 뒤 넣는다" 원칙에 어긋나 **표준 라이브러리만 쓴 Go 바이너리 1개(5.2MB, 250줄)** 로 했다.

## 흐름
```
git push(main) ─CI─▶ test(모든 push) ─▶ changes(pipeline/**·Dockerfile.collector 바뀜?) ─▶ build+push ghcr :latest, :sha-<short>
                                                                                              │
미니PC cron 5분 ─▶ mp2-cd ─▶ GHCR HEAD manifests/latest (익명 토큰, 이미지 안 받음) ─▶ digest 비교(state.json)
     ├─ 같음 → 끝
     ├─ 다름 & MODE=notify → Slack "새 이미지 감지" (12-02 판정 전 기본값)
     └─ 다름 & MODE=deploy → compose pull → up -d --no-deps 수집기 4개 → 75초 뒤 검증 → Slack 결과
                               검증 = 4개 Running + 재기동 뒤 [STATS] inserted>0. 실패 → 이전 이미지 ID 를 같은 태그로 되돌리고 up → Slack "실패+롤백"
```

- 설정 `~/mp2-secrets/cd.env`(600): MODE, SLACK_WEBHOOK_URL, IMAGE, TAG, COMPOSE_FILE, SERVICES, STATE, VERIFY_SEC.
- 상태 `data/logs/cd-state.json`: last_digest / notified_digest / deployed_digest / deployed_at / checked_at. 겹침은 `flock` 으로 막는다.
- 대상은 **수집기 4개만**. ClickHouse·Airflow 는 이 경로로 배포하지 않는다(상태 있는 컴포넌트는 사람이).
- 숫자(배포 때 Slack 에 함께): pull+up 초, 서비스별 첫 STATS 까지 초(= 수집 공백 상한), 전체 초.

## 전제와 순서
- compose 의 수집기 `image:` 는 11-25 에 `ghcr.io/aguantar/market-pipeline-v2-collector:latest` 로 바꿔 **커밋만** 했다. 현재 떠 있는 컨테이너는 로컬 빌드 `mp2-collector` 이미지 그대로다(관찰 중 재시작 금지). 12-02 판정 뒤 `MODE=deploy` 로 바꾸면 다음 digest 변화 때 처음 ghcr 이미지로 교체된다 — 그 첫 배포가 롤백 경로까지 포함한 실측이 된다.
- CI `changes` 잡: docs 커밋마다 수집기가 재배포되는 것을 막는다. `test` 는 모든 push 에서 돈다.
- 롤백 기준은 "검증 실패" 하나다. 배포 뒤 데이터 품질(대조·셀 비교)은 기존 DAG 가 본다.

## 11-25 실측
| 항목 | 값 |
|---|---|
| 바이너리 | 5,218,456 B, 정적, `CGO_ENABLED=0` |
| 단위 테스트 | 2 (설정 파싱·상태 왕복, short) |
| 1회 실행 | 약 1초 (토큰 + HEAD 2 요청) |
| 기준 digest | `a3e6da0e13cd` (13:47:24 UTC, Slack 기준 알림 1건 발송) |
| 배포·롤백 실측 | **12-02 이후** (관찰 중 재시작 금지) |

## 운영
- 켜기/끄기: `cd.env` 의 `MODE` 만 바꾼다(재시작 없음, 다음 cron 에서 반영).
- 로그 `data/logs/cd.log`. 수동 실행 `/home/calme/mp2-bin/mp2-cd`.
- 다시 빌드: `cd cd && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /home/calme/mp2-bin/mp2-cd .`
- 아직 안 한 것: 배포기 자체의 업데이트(바이너리는 사람이 빌드), ClickHouse·Airflow 이미지, 서명 검증(cosign). 셋 다 이 규모에서 비용 > 효용으로 판단, 필요가 생기면 여기 적는다.
