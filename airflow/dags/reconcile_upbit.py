"""06:35 UTC, 대상일 = ds(어제): Upbit REST 시간봉(진실값) → upbit_hourly_candles → dbt 대조 모델·테스트. v1 reconcile_trades 이식 (docs/00 §2).
풀 upbit_rest 1슬롯(초당 10회/IP 한도). 실패(유실 셀)만 Slack."""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from callbacks.slack_callbacks import send_health_alert
from mp2_common import DEFAULT_ARGS, dbt_cmd

MARKET_URL = "https://api.upbit.com/v1/market/all?is_details=false"
CANDLE_URL = "https://api.upbit.com/v1/candles/minutes/60"
LOSS_THRESHOLD_PCT = 99.0


def _target(context) -> str:
    return context["params"].get("target_date") or context["ds"]


def fetch_candles(**context) -> dict:
    from hooks.clickhouse_hook import ClickHouseHook
    day = datetime.strptime(_target(context), "%Y-%m-%d")
    to = (day + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    s = requests.Session(); s.headers["Accept"] = "application/json"
    markets = [m["market"] for m in s.get(MARKET_URL, timeout=15).json() if m["market"].startswith("KRW-")]
    rows, failed = [], []
    for m in markets:
        for attempt in range(4):
            r = s.get(CANDLE_URL, params={"market": m, "to": to, "count": 24}, timeout=15)
            if r.status_code == 429:
                time.sleep(1.0 * (attempt + 1)); continue
            break
        if r.status_code != 200:
            failed.append(f"{m}:{r.status_code}")
        else:
            for c in r.json():
                if c["candle_date_time_utc"].startswith(day.strftime("%Y-%m-%d")):
                    rows.append({"market": m, "hour_utc": c["candle_date_time_utc"].replace("T", " "), "open": c["opening_price"],
                                 "high": c["high_price"], "low": c["low_price"], "close": c["trade_price"],
                                 "volume": c["candle_acc_trade_volume"], "amount": c["candle_acc_trade_price"]})
        time.sleep(0.13)
    if rows:
        ClickHouseHook().execute("INSERT INTO mp2.upbit_hourly_candles FORMAT JSONEachRow\n" + "\n".join(json.dumps(r) for r in rows))
    if len(failed) > len(markets) * 0.05:
        raise RuntimeError(f"candle fetch failed {len(failed)}/{len(markets)}: {failed[:10]}")
    return {"date": _target(context), "markets": len(markets), "rows": len(rows), "failed": len(failed)}


def summarize(**context) -> dict:
    from hooks.clickhouse_hook import ClickHouseHook
    day = _target(context); h = ClickHouseHook()
    agg = h.get_first(f"SELECT round(100*sum(ch_vol)/sum(candle_vol),2) AS weighted_pct, count() AS cells, countIf(ratio_pct < {LOSS_THRESHOLD_PCT}) AS cells_below, uniqExact(market) AS markets "
                      f"FROM mp2.int_upbit_reconcile_hourly WHERE toDate(hour_utc) = '{day}' AND candle_vol > 0") or {}
    worst = h.get_records(f"SELECT market, hour_utc, round(ratio_pct,2) AS ratio_pct, ch_n FROM mp2.int_upbit_reconcile_hourly "
                          f"WHERE toDate(hour_utc) = '{day}' AND candle_vol > 0 AND ratio_pct < {LOSS_THRESHOLD_PCT} ORDER BY ratio_pct LIMIT 10")
    dbt_failed = context["dag_run"].get_task_instance("dbt_reconcile").state != "success"
    result = {"date": day, **agg, "worst": worst, "dbt_failed": dbt_failed}
    context["ti"].log.info("reconcile summary: %s", result)
    if dbt_failed or int(agg.get("cells_below") or 0) > 0:
        send_health_alert([{"name": "reconcile_upbit", "status": "critical",   # 플러그인 계약은 name/message (11-25 KeyError 재발 지점)
                            "message": f"{day} 가중 {agg.get('weighted_pct')}% · 셀 {agg.get('cells')} 중 {agg.get('cells_below')} 유실 · worst {worst[:3]}"}])
        raise RuntimeError("reconcile failed - see int_upbit_reconcile_hourly")
    return result


with DAG("reconcile_upbit", default_args=DEFAULT_ARGS, schedule="35 6 * * *", start_date=datetime(2026, 11, 24), catchup=False,
         max_active_runs=1, tags=["reconcile", "upbit"], params={"target_date": ""}) as dag:
    f = PythonOperator(task_id="fetch_hourly_candles", python_callable=fetch_candles, pool="upbit_rest")
    d = BashOperator(task_id="dbt_reconcile", pool="dbt",
                     bash_command=dbt_cmd("build", select="int_upbit_reconcile_hourly+", vars_json='{"reconcile_date": "{{ params.target_date or ds }}"}'))
    s = PythonOperator(task_id="summarize", python_callable=summarize, trigger_rule="all_done")
    f >> d >> s
