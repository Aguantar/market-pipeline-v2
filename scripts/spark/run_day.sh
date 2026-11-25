#!/usr/bin/env bash
# Spark(local) 하루치 재파싱 1회 실행 래퍼 (docs/01 §10 #7). 결과는 mp2.compaction_runs(engine='spark') + 로그 파일.
# 사용: scripts/spark/run_day.sh 2026-11-24   (관찰 중엔 KST 심야 1회만, 라이브 경로는 건드리지 않는다 — 파일 읽기 + parquet_spark/ 쓰기 + CH 검증 조회)
set -uo pipefail
DS=${1:?ds}
REPO=/home/calme/market-pipeline-v2
LOG=$REPO/data/logs/spark-$DS.log
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}
export PATH=$JAVA_HOME/bin:/usr/local/bin:/usr/bin:/bin
CH_PASSWORD=$(grep -m1 '^CLICKHOUSE_PASSWORD=' $REPO/.env | cut -d= -f2-); export CH_PASSWORD
{
  echo "=== spark day run ds=$DS start $(date -u +%FT%TZ) host_avail_mb=$(free -m | awk '/Mem/{print $7}') load=$(cut -d' ' -f1-3 /proc/loadavg)"
  java -version 2>&1 | head -1
  cd $REPO && /usr/bin/time -v $REPO/.venv/bin/python spark/export_day_spark.py --day "$DS" --data-root $REPO/data --cores 2 --driver-mem 2g 2>&1 \
    | grep -vE "WARN (NativeCodeLoader|Utils|GarbageCollectionMetrics)|^\s*$"
  echo "=== end $(date -u +%FT%TZ)"
  docker exec mp2-clickhouse clickhouse-client --password "$CH_PASSWORD" -q "SELECT run_at, engine, stream, out_rows, formatReadableSize(out_bytes) size, verified, round(seconds) s, round(peak_mem_mb) mem FROM mp2.compaction_runs WHERE day='$DS' AND (engine='spark' OR run_at >= '2026-11-25 02:53:00') ORDER BY stream, engine FORMAT PrettyCompactMonoBlock"
} >> "$LOG" 2>&1
