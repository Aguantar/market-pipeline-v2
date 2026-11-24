"""매시 05분: dbt build (1분 집계는 어제·오늘 파티션 교체, 품질 표 재계산, 테스트). 풀 dbt 1슬롯으로 직렬화 (v1 docs/44 §6)."""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from mp2_common import DEFAULT_ARGS, dbt_cmd

with DAG("dbt_hourly", default_args=DEFAULT_ARGS, schedule="5 * * * *", start_date=datetime(2026, 11, 24), catchup=False,
         max_active_runs=1, tags=["dbt"]) as dag:
    BashOperator(task_id="dbt_build", pool="dbt", bash_command=dbt_cmd("build"))
