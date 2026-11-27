"""00:30 UTC: 어제(ds) 파티션을 run_date 로 확정 재계산 + 테스트. 과거 날짜는 `airflow dags backfill` 또는 trigger with conf target_date."""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from mp2_common import DEFAULT_ARGS, dbt_cmd

with DAG("dbt_daily_finalize", default_args=DEFAULT_ARGS, schedule="30 0 * * *", start_date=datetime(2026, 11, 24), catchup=False,
         max_active_runs=1, tags=["dbt"], params={"target_date": ""}) as dag:
    day = "{{ params.target_date or ds }}"
    BashOperator(task_id="dbt_run_day", pool="dbt",
                 bash_command=dbt_cmd("run", select="int_upbit_orderbook_1m int_upbit_ohlcv_1m int_binance_ohlcv_1m dq_ingest_daily dq_collector_gaps_daily", vars_json=f'{{"run_date": "{day}"}}'))   # dq 2개는 11-27 부터 여기서(dbt_hourly 가 전체 build 를 그만둠, #10)
