#!/usr/bin/env bash
# 5분 관측 스냅샷 (호스트 cron). v1 scripts/observe/collect_metrics.sh 의 v2 판.
# 같은 단위(5분 행수, 지연 p50/p95, 메모리, 디스크)로 적어 docs/02 전·후 비교에 쓴다.
# 읽기 전용: ClickHouse 조회 + docker stats + df/du. 결과는 mp2.ops_metrics_5m 와 CSV 사본 두 곳.
set -uo pipefail
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
REPO=/home/calme/market-pipeline-v2
CSV=/home/calme/pipeline-observation/mp2_metrics_5m.csv
PW=$(grep -m1 '^CLICKHOUSE_PASSWORD=' "$REPO/.env" | cut -d= -f2-)
CH() { docker exec mp2-clickhouse clickhouse-client --password "$PW" -q "$1" 2>/dev/null; }

TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)
TS_CH=$(date -u -d "$TS" +'%Y-%m-%d %H:%M:%S')   # ClickHouse DateTime 리터럴은 'Z' 접미를 못 읽는다
[ -f "$CSV" ] || echo "ts,stream,rows_5m,keys_5m,lag_p50_ms,lag_p95_ms,lag_max_ms,freshness_ms,gaps_1h,container_mem_mb,ch_mem_mb,host_used_mb,disk_free_gb,raw_dir_gb,parquet_dir_gb" > "$CSV"

# 컨테이너 메모리(MiB). docker stats 는 "123.4MiB / 384MiB" 또는 "1.2GiB / 3GiB" 형태.
declare -A MEM
while read -r name usage; do
  case "$usage" in
    *GiB) MEM[$name]=$(awk -v v="${usage%GiB}" 'BEGIN{printf "%.1f", v*1024}') ;;
    *MiB) MEM[$name]=${usage%MiB} ;;
    *KiB) MEM[$name]=$(awk -v v="${usage%KiB}" 'BEGIN{printf "%.1f", v/1024}') ;;
    *)    MEM[$name]=0 ;;
  esac
done < <(docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' 2>/dev/null | awk '{print $1, $2}')

HOST_USED=$(free -m | awk '/Mem/{print $3}')
DISK_FREE=$(df -BG "$REPO/data" | awk 'NR==2{gsub(/G/,"",$4); print $4}')
RAW_GB=$(du -sm "$REPO/data/raw" 2>/dev/null | awk '{printf "%.2f",$1/1024}')
PQ_GB=$(du -sm "$REPO/data/parquet" 2>/dev/null | awk '{printf "%.2f",$1/1024}')

VALUES=""
# stream : table : key col : event time col : container name
for S in \
  "upbit/trades:raw_upbit_trades:market:trade_ts:mp2-upbit-trades" \
  "upbit/orderbook:raw_upbit_orderbook:market:ts:mp2-upbit-orderbook" \
  "binance/trades:raw_binance_trades:symbol:trade_ts:mp2-binance-trades" \
  "binance/depth:raw_binance_depth:symbol:event_ts:mp2-binance-depth"; do
  IFS=: read -r stream table key tcol cname <<< "$S"
  src=${stream%/*}; st=${stream#*/}
  R=$(CH "SELECT count(), uniqExact($key),
      round(quantile(0.5)(toUnixTimestamp64Milli(recv_ts) - toUnixTimestamp64Milli($tcol)), 1),
      round(quantile(0.95)(toUnixTimestamp64Milli(recv_ts) - toUnixTimestamp64Milli($tcol)), 1),
      max(toUnixTimestamp64Milli(recv_ts) - toUnixTimestamp64Milli($tcol)),
      toUnixTimestamp64Milli(now64(3)) - toUnixTimestamp64Milli(max(recv_ts)),
      (SELECT count() FROM mp2.collector_gaps WHERE source = '$src' AND stream = '$st' AND inserted_at >= now64() - INTERVAL 1 HOUR)
    FROM mp2.$table
    WHERE recv_ts >= now64() - INTERVAL 5 MINUTE AND $tcol >= now64() - INTERVAL 2 HOUR
    FORMAT TSV")
  IFS=$'\t' read -r n k p50 p95 mx fresh gaps <<< "${R:-$'\t\t\t\t\t\t'}"
  n=${n:-0}; k=${k:-0}; p50=${p50:-0}; p95=${p95:-0}; mx=${mx:-0}; fresh=${fresh:-0}; gaps=${gaps:-0}; cm=${MEM[$cname]:-0}
  VALUES+="('$TS_CH','$stream',$n,$k,$p50,$p95,$mx,$fresh,$gaps,$cm,0,$HOST_USED,$DISK_FREE,$RAW_GB,$PQ_GB),"
  echo "$TS,$stream,$n,$k,$p50,$p95,$mx,$fresh,$gaps,$cm,,$HOST_USED,$DISK_FREE,$RAW_GB,$PQ_GB" >> "$CSV"
done
CHM=${MEM[mp2-clickhouse]:-0}
VALUES+="('$TS_CH','host',0,0,0,0,0,0,0,0,$CHM,$HOST_USED,$DISK_FREE,$RAW_GB,$PQ_GB)"
echo "$TS,host,0,0,0,0,0,0,0,,$CHM,$HOST_USED,$DISK_FREE,$RAW_GB,$PQ_GB" >> "$CSV"

CH "INSERT INTO mp2.ops_metrics_5m (ts, stream, rows_5m, keys_5m, lag_p50_ms, lag_p95_ms, lag_max_ms, freshness_ms, gaps_1h, container_mem_mb, ch_mem_mb, host_used_mb, disk_free_gb, raw_dir_gb, parquet_dir_gb) VALUES $VALUES" \
  || echo "$TS insert failed"
