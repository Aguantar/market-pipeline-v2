"""10분마다: 적재 생존·공백·디스크. 결과는 mp2.pipeline_incidents 에 남기고 critical 만 Slack. v1 16항목 중 이 스택에 남는 것만 (docs/01-design §6)."""
from __future__ import annotations

import json
import os
import shutil
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from callbacks.slack_callbacks import send_health_alert
from mp2_common import DEFAULT_ARGS

STREAMS = {"upbit/trades": ("raw_upbit_trades", "trade_ts"), "upbit/orderbook": ("raw_upbit_orderbook", "ts"),
           "binance/trades": ("raw_binance_trades", "trade_ts"), "binance/depth": ("raw_binance_depth", "event_ts")}


def check(**context) -> dict:
    from hooks.clickhouse_hook import ClickHouseHook
    h = ClickHouseHook(); issues = []
    for stream, (table, tcol) in STREAMS.items():
        n = int(h.get_first(f"SELECT count() AS n FROM mp2.{table} WHERE recv_ts >= now64() - INTERVAL 10 MINUTE AND {tcol} >= now64() - INTERVAL 1 HOUR")["n"])
        if n == 0:
            issues.append({"component": stream, "status": "critical", "message": "최근 10분 적재 0건 (수집기 정지 또는 ClickHouse 적재 실패)"})
    gaps = h.get_records("SELECT source, stream, count() AS n, round(sum(toUnixTimestamp64Milli(to_ts)-toUnixTimestamp64Milli(from_ts))/1000) AS sec "
                         "FROM mp2.collector_gaps WHERE inserted_at >= now64() - INTERVAL 1 HOUR GROUP BY source, stream")
    for g in gaps:
        if int(g["n"]) >= 3 or float(g["sec"]) >= 300:
            issues.append({"component": f"{g['source']}/{g['stream']}", "status": "warning", "message": f"최근 1시간 재연결 {g['n']}회, 공백 {g['sec']}초"})
    free_gb = shutil.disk_usage(os.environ.get("MP2_DATA_ROOT", "/data/raw")).free / 2**30
    if free_gb < 50:
        issues.append({"component": "disk", "status": "critical" if free_gb < 20 else "warning", "message": f"원본 디스크 여유 {free_gb:.0f}GB"})
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    if issues:
        rows = [{"detected_at": now, "stage": "health", "component": i["component"], "severity": i["status"], "title": "health_check",
                 "detail": i["message"], "source": "health_check", "dedup_key": f"{i['component']}:{now[:13]}"} for i in issues]
        h.execute("INSERT INTO mp2.pipeline_incidents FORMAT JSONEachRow\n" + "\n".join(json.dumps(r, ensure_ascii=False) for r in rows))
        crit = [i for i in issues if i["status"] == "critical"]
        if crit:
            # 플러그인(v1) 계약은 {name, message}. 11-24 05:10~06:10 KeyError('name') 로 critical 7건이 발송되지 않았다 (worklog 11-25).
            send_health_alert([{"name": i["component"], "message": i["message"]} for i in crit])
    context["ti"].log.info("health: %d issue(s) %s free_gb=%.1f", len(issues), issues, free_gb)
    return {"issues": len(issues), "free_gb": round(free_gb, 1)}


with DAG("health_check", default_args={**DEFAULT_ARGS, "retries": 0}, schedule="*/10 * * * *", start_date=datetime(2026, 11, 24), catchup=False,
         max_active_runs=1, tags=["ops"]) as dag:
    PythonOperator(task_id="check", python_callable=check)
