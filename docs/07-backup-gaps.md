# 07. 백업 빈 곳 메우기 (2026-11-25)

v1 종료 뒤 미니PC 에 남은 상태 중 백업이 없던 것 둘(my-postgres, /etc/caddy)과, Oracle 블록 볼륨 하나에만 있던 오프사이트 사본의 2차 사본(Object Storage).

## 1. 무엇이 비어 있었나
| 대상 | 내용 | 전 | 후 |
|---|---|---|---|
| `my-postgres` | blood_db 62MB·metabase 30MB·icepush 7MB·postgres 7MB (계정 calme) | 백업 없음. 11-23 games pg_dump 1회만 | 매일 01:40 UTC `pg_dumpall --clean --if-exists` gzip → Oracle, 14일 |
| `/etc/caddy` | Caddyfile + .bak 11개(05-10~11-23) | 백업 없음, 이력은 .bak 파일명 | **git 이력**(`~/minipc-config`, GitHub 비공개) + 매일 tar → Oracle |
| Oracle `/mnt/backup` 자체 | v1 final 29GB, mp2 Parquet, minipc-final 등 - 블록 볼륨 1개(147GB 중 111GB) | 사본 1개 | v2 컬럼형 Parquet 최근 7일을 Object Storage(Always Free 20GB)에 2차 사본 - §3 |

## 2. 호스트 백업 `scripts/ops/backup-host.sh` (cron 01:40 UTC)
- Airflow 컨테이너에는 docker 소켓이 없어 DAG 로는 `docker exec my-postgres` 를 못 한다 → 호스트 cron. 백업 대상이 호스트 것이니 호스트가 맞다.
- 순서: pg_dumpall(0바이트면 실패 처리) → `tar /etc/caddy` → SHA256SUMS → rsync → **dry-run 차이 0 + 원격 `sha256sum -c`** → 로컬·원격 14일 보존. 둘 중 하나라도 어긋나면 종료 코드 1.
- 11-25 첫 실행: **8.6MB, 4초, rsync 차이 0, 원격 SHA 일치**, 덤프 안 `CREATE DATABASE` 5개. Oracle `/mnt/backup/minipc-host/2026-11-25/`.
- 복원: `zcat my-postgres.pg_dumpall.sql.gz | docker exec -i my-postgres psql -U calme -d postgres` (--clean 이라 기존 객체를 지우고 다시 만든다 - 다른 호스트의 빈 Postgres 에 하는 게 안전).

## 3. Caddyfile → git
- `~/minipc-config`(GitHub 비공개 `Aguantar/minipc-config`): .bak 11개를 **mtime 순으로 커밋 날짜를 그대로 둔 이력 13커밋**으로 옮겼다. HEAD = 현재 `/etc/caddy/Caddyfile`(바이트 동일 확인).
- 비밀값 점검: 평문 password/secret/token 0건. basic_auth bcrypt 해시 4종만 있음(평문 아님, 비공개 저장소 - 사용자 결정).
- 원본 .bak 삭제는 root 소유라 사용자가 sudo 로(§5). 삭제 뒤 `/etc/caddy` 는 Caddyfile 1개.
- 적용 절차는 저장소 README 에: cp → `caddy validate` → `systemctl reload caddy`.

## 4. Object Storage 2차 사본 (설계, 키 받으면 실행)
- 대상: **v2 컬럼형 Parquet 최근 7일**(≈2.3GB/일 × 7 ≈ 16GB < 20GB). 이유: Parquet 가 파싱된 원본이고, 거기서 ClickHouse 하루치를 63초에 되살릴 수 있음을 docs/52·11-25 rebuild 로 확인했다. 일 1회 동기화라 API 요청도 월 5만 한도의 1% 미만.
- 도구: **rclone v1.76.0**(Oracle `~/bin/rclone`, 공식 zip SHA256 검증, sudo 없음). oci cli 는 Python 의존이 커서 제외. S3 호환 엔드포인트 `https://<namespace>.compat.objectstorage.ap-chuncheon-1.oraclecloud.com`.
- 흐름: Oracle cron 02:00 UTC `rclone sync /mnt/backup/mp2/parquet oci:mp2-parquet --max-age 8d` → `rclone check`(크기·해시) → Slack 결과. 보존은 소스(14일)보다 짧게 7일: 버킷 쪽 lifecycle 7일 삭제 규칙.
- 복원 연습 1회: 버킷에서 하루치 4파일 내려받기 → `file()` 로 ClickHouse 임시 DB 적재 → 라이브와 셀 대조 → 시간 기록. 결과는 이 문서 §6 에.

## 5. 사용자가 할 것
1. `.bak` 정리(git 이력 확인 뒤): `sudo rm /etc/caddy/Caddyfile.bak*`
2. cron: `40 1 * * * /home/calme/market-pipeline-v2/scripts/ops/backup-host.sh >> /home/calme/market-pipeline-v2/data/logs/backup-host.log 2>&1`
3. OCI 콘솔(리전 **ap-chuncheon-1**, 인스턴스와 같은 리전): 버킷 `mp2-parquet`(Standard, 비공개, 버전 관리 끔) → 프로필 → 사용자 설정 → **고객 비밀 키(Customer Secret Keys)** 생성 → Access Key·Secret Key, 그리고 테넌시 상세의 **Object Storage 네임스페이스** 세 값을 Oracle 의 `~/.config/rclone/rclone.conf`(600) 에 아래 형식으로. 값은 저장소·채팅에 적지 말고 파일에만.
```
[oci]
type = s3
provider = Other
access_key_id = <Access Key>
secret_access_key = <Secret Key>
region = ap-chuncheon-1
endpoint = https://<namespace>.compat.objectstorage.ap-chuncheon-1.oraclecloud.com
```

## 6. 실측 (채워 넣음)
| 항목 | 값 |
|---|---|
| backup-host 첫 실행 | 8.6MB / 4초 / 차이 0 / SHA 일치 (11-25 13:59 UTC) |
| Caddy 이력 커밋 | 13 (2026-05-10 ~ 11-23) |
| Object Storage 첫 동기화 | (키 받은 뒤) |
| 복원 연습 | (키 받은 뒤) |
