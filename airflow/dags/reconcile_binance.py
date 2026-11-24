"""00:40 UTC, 대상일 = ds(어제): Binance 1h 캔들 체결수(진실값) → binance_hourly_candles → dbt 대조. v1 reconcile_binance 이식."""
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

REST = "https://api.binance.com"


def _target(context) -> str:
    return context["params"].get("target_date") or context["ds"]


def fetch_candles(**context) -> dict:
    from hooks.clickhouse_hook import ClickHouseHook
    day = _target(context); h = ClickHouseHook()
    start = int(datetime.strptime(day, "%Y-%m-%d").timestamp() * 1000); end = start + 86400_000 - 1
    symbols = [r["symbol"] for r in h.get_records(f"SELECT DISTINCT symbol FROM mp2.raw_binance_trades WHERE toDate(trade_ts) = '{day}'")]
    s = requests.Session(); rows, failed = [], []
    for sym in symbols:
        try:
            r = s.get(f"{REST}/api/v3/klines", params={"symbol": sym, "interval": "1h", "startTime": start, "endTime": end, "limit": 24}, timeout=15)
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", "5")))
                r = s.get(f"{REST}/api/v3/klines", params={"symbol": sym, "interval": "1h", "startTime": start, "endTime": end, "limit": 24}, timeout=15)
            r.raise_for_status()
            for k in r.json():
                rows.append({"symbol": sym, "hour_utc": datetime.utcfromtimestamp(k[0] / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                             "open": float(k[1]), "high": float(k[2]), "low": float(k[3]), "close": float(k[4]),
                             "volume": float(k[5]), "quote_volume": float(k[7]), "trade_count": int(k[8])})
        except Exception as e:  # noqa: BLE001
            failed.append(f"{sym}:{type(e).__name__}")
        time.sleep(0.15)
    if rows:
        h.execute("INSERT INTO mp2.binance_hourly_candles FORMAT JSONEachRow\n" + "\n".join(json.dumps(r) for r in rows))
    context["ti"].log.info("day %s symbols %d rows %d failed %d %s", day, len(symbols), len(rows), len(failed), failed[:5])
    return {"day": day, "symbols": len(symbols), "rows": len(rows), "failed": len(failed)}


def summarize(**context) -> dict:
    from hooks.clickhouse_hook import ClickHouseHook
    day = _target(context); h = ClickHouseHook()
    agg = h.get_first(f"SELECT round(100*sum(ours_n)/sum(ex_n),3) AS weighted_pct, count() AS cells, countIf(ratio_pct < 99) AS cells_below, countIf(ours_n = 0) AS no_rows "
                      f"FROM mp2.int_binance_reconcile_hourly WHERE toDate(hour_utc) = '{day}' AND ex_n > 0") or {}
    context["ti"].log.info("binance reconcile %s: %s", day, agg)
    if int(agg.get("cells_below") or 0) > 0:
        send_health_alert([{"component": "reconcile_binance", "status": "critical",
                            "message": f"{day} 가중 {agg.get('weighted_pct')}% · 셀 {agg.get('cells')} 중 {agg.get('cells_below')} <99%"}])
    return agg


with DAG("reconcile_binance", default_args=DEFAULT_ARGS, schedule="40 0 * * *", start_date=datetime(2026, 11, 24), catchup=False,
         max_active_runs=1, tags=["reconcile", "binance"], params={"target_date": ""}) as dag:
    f = PythonOperator(task_id="fetch_hourly_candles", python_callable=fetch_candles)
    d = BashOperator(task_id="dbt_reconcile", pool="dbt",
                     bash_command=dbt_cmd("build", select="int_binance_reconcile_hourly+", vars_json='{"reconcile_date": "{{ params.target_date or ds }}"}'))
    s = PythonOperator(task_id="summarize", python_callable=summarize)
    f >> d >> s
