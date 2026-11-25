"""07:10 UTC 매일: 알림 경로 카나리 (docs/06). health_check 와 같은 send_health_alert 경로로 "alert-path ok" 1건을 실제 발송하고,
Slack 이 2xx 를 돌려줄 때만 /data/logs/alert_canary.json 에 시각을 적는다. heartbeat-push.sh 가 그 시각을 Oracle 감시자에게 실어 보내고,
감시자는 26시간 넘게 갱신이 없으면 degraded 로 알린다 — 알림을 보내는 코드가 죽어도(11-24 KeyError 7건 미발송) 밖에서 안다."""
import json
import os
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from callbacks.slack_callbacks import send_health_alert
from mp2_common import DEFAULT_ARGS

MARKER = os.environ.get("MP2_CANARY_MARKER", "/data/logs/alert_canary.json")


def canary(**context) -> dict:
    now = datetime.now(timezone.utc)
    ok = send_health_alert([{"name": "alert-path", "message": f"ok {now:%Y-%m-%d %H:%M}Z (alert_canary, 매일 07:10 UTC)"}])
    if not ok:
        raise RuntimeError("Slack 발송 실패 (2xx 아님) - 알림 경로 점검")
    tmp = MARKER + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"sent_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "epoch": int(now.timestamp()), "run_id": context["run_id"]}, f)
    os.replace(tmp, MARKER)
    context["ti"].log.info("canary delivered, marker %s", MARKER)
    return {"sent_at": now.isoformat()}


with DAG("alert_canary", default_args=DEFAULT_ARGS, schedule="10 7 * * *", start_date=datetime(2026, 11, 25), catchup=False,
         max_active_runs=1, tags=["ops", "alerting"]) as dag:
    PythonOperator(task_id="send_canary", python_callable=canary)
