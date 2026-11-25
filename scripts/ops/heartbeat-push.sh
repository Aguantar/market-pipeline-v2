#!/usr/bin/env bash
# 미니PC 생존 신호를 Oracle 감시자(cdc-watchdog)로 push. 호스트 cron 5분. v1 scripts/ops/heartbeat-push.sh 를 mp2 테이블로 바꾼 것.
# 감시자 계약(cdc-watchdog.py): sent_at_epoch 나이로 down, clickhouse!=ok → degraded, verdict==stale → stale. 필드 이름은 v1 그대로 유지.
set -uo pipefail
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
REPO=/home/calme/market-pipeline-v2
REMOTE=${HB_REMOTE:-ubuntu@10.88.0.1}
KEY=${HB_KEY:-/home/calme/.ssh/oci_key}
RDIR=${HB_RDIR:-/home/ubuntu/cdc-watchdog}
MODE=${1:-push}
CP=$(grep -m1 '^CLICKHOUSE_PASSWORD=' "$REPO/.env" | cut -d= -f2-)
# 생존 지표라 FINAL 없이 센다(머지 전 중복 포함). 파티션 조건은 프루닝용.
SQL="SELECT
  (SELECT count() FROM mp2.raw_upbit_trades   WHERE recv_ts >= now64() - INTERVAL 5 MINUTE AND trade_ts >= now64() - INTERVAL 1 HOUR) AS recent_trades_5m,
  (SELECT count() FROM mp2.raw_upbit_orderbook WHERE recv_ts >= now64() - INTERVAL 5 MINUTE AND ts >= now64() - INTERVAL 1 HOUR)      AS recent_orderbook_5m,
  (SELECT count() FROM mp2.raw_binance_trades WHERE recv_ts >= now64() - INTERVAL 5 MINUTE AND trade_ts >= now64() - INTERVAL 1 HOUR) AS recent_binance_5m,
  (SELECT count() FROM mp2.raw_upbit_trades) AS total_trades
FORMAT JSON"
CH_OUT=$(curl -s -m 10 -u "default:$CP" -w '\n%{http_code}' 'http://127.0.0.1:8123/' --data-binary "$SQL" 2>/dev/null)
CH_RC=$?
CH_CODE=$(printf '%s' "$CH_OUT" | tail -n1)
CH_BODY=$(printf '%s' "$CH_OUT" | sed '$d')
PAYLOAD=$(CH_BODY="$CH_BODY" CH_RC="$CH_RC" CH_CODE="$CH_CODE" CANARY="$REPO/data/logs/alert_canary.json" python3 - <<'PY'
import json, os, socket, time
body, rc, code = os.environ["CH_BODY"], os.environ["CH_RC"], os.environ.get("CH_CODE", "")
now = time.time()
out = {"schema": 2, "pipeline": "mp2", "host": socket.gethostname(),
       "sent_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)), "sent_at_epoch": int(now),
       "clickhouse": "fail", "verdict": "degraded", "detail": ""}
# 알림 경로 카나리(alert_canary DAG 가 Slack 2xx 를 받은 시각). 감시자가 26시간 넘게 갱신 없으면 degraded (docs/06)
try:
    c = json.load(open(os.environ["CANARY"]))
    out["alert_canary_at"], out["alert_canary_epoch"] = c["sent_at"], int(c["epoch"])
except Exception:
    pass
try:
    row = json.loads(body)["data"][0]
    out.update({"clickhouse": "ok", "recent_trades_5m": int(row["recent_trades_5m"]), "recent_orderbook_5m": int(row["recent_orderbook_5m"]),
                "recent_binance_5m": int(row["recent_binance_5m"]), "total_trades": int(row["total_trades"])})
    # stale: 호스트·CH 는 살았는데 적재가 멎음. Upbit 체결은 새벽에 5분간 0 일 수 있어 세 스트림이 모두 0 일 때만.
    out["verdict"] = "stale" if (out["recent_trades_5m"] + out["recent_orderbook_5m"] + out["recent_binance_5m"]) == 0 else "ok"
except Exception as e:
    hint = {"403": "인증 실패", "000": "접속 불가 (컨테이너 확인)", "500": "ClickHouse 내부 오류", "": "응답 없음"}.get(code, f"HTTP {code}")
    out["detail"] = f"HTTP {code or '없음'} - {hint} (curl rc={rc}): {type(e).__name__}"
print(json.dumps(out, ensure_ascii=False))
PY
)
[ "$MODE" = "--stdout" ] && { echo "$PAYLOAD"; exit 0; }
if printf '%s\n' "$PAYLOAD" | ssh -i "$KEY" -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=no \
     "$REMOTE" "mkdir -p $RDIR && cat > $RDIR/heartbeat.json.tmp && mv $RDIR/heartbeat.json.tmp $RDIR/heartbeat.json" 2>/dev/null; then
  echo "$(date -u +%FT%TZ) sent $(echo "$PAYLOAD" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d["verdict"], "ch="+d["clickhouse"], "trades5m="+str(d.get("recent_trades_5m","-")), "ob5m="+str(d.get("recent_orderbook_5m","-")), "bn5m="+str(d.get("recent_binance_5m","-")))')"
else
  echo "$(date -u +%FT%TZ) PUSH FAILED (oracle unreachable or tunnel down)"; exit 1
fi
