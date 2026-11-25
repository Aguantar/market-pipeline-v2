#!/usr/bin/env bash
# 미니PC 호스트 쪽 백업 빈 곳 (docs/07): my-postgres(blood_db·metabase·icepush) pg_dumpall + /etc/caddy → Oracle /mnt/backup/minipc-host/<UTC일자>/
# Airflow 컨테이너엔 docker 소켓이 없어 DAG 로 못 하므로 호스트 cron(매일 01:40 UTC). 검증 = rsync dry-run 차이 0 + SHA256 원격 재계산 일치. 보존 14일(로컬·원격).
set -uo pipefail
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
REPO=/home/calme/market-pipeline-v2
DAY=$(date -u +%F)
LOCAL=$REPO/data/backup-host/$DAY
REMOTE=${HB_REMOTE:-ubuntu@10.88.0.1}
KEY=${HB_KEY:-/home/calme/.ssh/oci_key}
RROOT=/mnt/backup/minipc-host
KEEP_DAYS=14
SSH="ssh -i $KEY -o BatchMode=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=no"
T0=$(date +%s)
mkdir -p "$LOCAL" || exit 1

# 1) my-postgres: 전체 덤프(롤·DB 포함). 컨테이너 밖으로 스트림 → gzip. 실패 시 0 바이트 파일을 남기지 않는다.
PGU=$(docker inspect my-postgres --format '{{range .Config.Env}}{{println .}}{{end}}' | awk -F= '/^POSTGRES_USER=/{print $2}')
if docker exec my-postgres pg_dumpall -U "${PGU:-postgres}" --clean --if-exists 2>"$LOCAL/pg_dumpall.err" | gzip -6 > "$LOCAL/my-postgres.pg_dumpall.sql.gz.tmp" && [ -s "$LOCAL/my-postgres.pg_dumpall.sql.gz.tmp" ]; then
  mv "$LOCAL/my-postgres.pg_dumpall.sql.gz.tmp" "$LOCAL/my-postgres.pg_dumpall.sql.gz"; rm -f "$LOCAL/pg_dumpall.err"
else
  echo "$(date -u +%FT%TZ) pg_dumpall FAILED: $(head -c 200 "$LOCAL/pg_dumpall.err")"; rm -f "$LOCAL/my-postgres.pg_dumpall.sql.gz.tmp"; exit 1
fi
# 2) /etc/caddy 전체(Caddyfile + 남은 .bak). 읽기 권한만 필요(644).
tar -C /etc -czf "$LOCAL/etc-caddy.tar.gz" caddy || { echo "$(date -u +%FT%TZ) caddy tar FAILED"; exit 1; }
# 3) 목록·해시
( cd "$LOCAL" && sha256sum my-postgres.pg_dumpall.sql.gz etc-caddy.tar.gz > SHA256SUMS )
# 4) 전송 + 검증
$SSH "$REMOTE" "mkdir -p $RROOT/$DAY" || { echo "$(date -u +%FT%TZ) remote mkdir FAILED (tunnel?)"; exit 1; }
rsync -rlt -e "$SSH" "$LOCAL/" "$REMOTE:$RROOT/$DAY/" || { echo "$(date -u +%FT%TZ) rsync FAILED"; exit 1; }
DIFF=$(rsync -rltn --itemize-changes -e "$SSH" "$LOCAL/" "$REMOTE:$RROOT/$DAY/" | grep -c '^[<>ch]')
RSHA=$($SSH "$REMOTE" "cd $RROOT/$DAY && sha256sum -c SHA256SUMS --quiet && echo ok || echo MISMATCH")
# 5) 보존
find "$REPO/data/backup-host" -mindepth 1 -maxdepth 1 -type d -mtime +$KEEP_DAYS -exec rm -rf {} + 2>/dev/null
$SSH "$REMOTE" "cutoff=\$(date -u -d '-$KEEP_DAYS days' +%F); for d in \$(ls -d $RROOT/20* 2>/dev/null); do [[ \"\${d##*/}\" < \"\$cutoff\" ]] && rm -rf \"\$d\"; done; du -sh $RROOT | cut -f1"
SZ=$(du -sh "$LOCAL" | cut -f1)
echo "$(date -u +%FT%TZ) backup-host $DAY size=$SZ rsync_diff=$DIFF remote_sha=$RSHA secs=$(( $(date +%s) - T0 ))"
[ "$DIFF" = "0" ] && [ "$RSHA" = "ok" ] || exit 1
