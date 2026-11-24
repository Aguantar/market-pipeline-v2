"""DAG 공통: dbt 명령, Slack 콜백, 기본 인자."""
from __future__ import annotations

import os
from datetime import timedelta

from callbacks.slack_callbacks import task_failure_callback

DBT_DIR = "/opt/airflow/dbt"
DBT = f"cd {DBT_DIR} && dbt"
DBT_PROFILES = f"--profiles-dir {DBT_DIR}"

DEFAULT_ARGS = {
    "owner": "calme",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_callback,
}


def dbt_cmd(sub: str, select: str | None = None, vars_json: str | None = None) -> str:
    cmd = f"{DBT} {sub} {DBT_PROFILES}"
    if select:
        cmd += f" --select {select}"
    if vars_json:
        cmd += f" --vars '{vars_json}'"
    return cmd + " 2>&1"
