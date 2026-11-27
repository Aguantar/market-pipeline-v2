"""매시 05분: dbt build (1분 집계는 어제·오늘 파티션 교체, 품질 표 재계산, 테스트). 풀 dbt 1슬롯으로 직렬화 (v1 docs/44 §6)."""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from mp2_common import DEFAULT_ARGS, dbt_cmd

with DAG("dbt_hourly", default_args=DEFAULT_ARGS, schedule="5 * * * *", start_date=datetime(2026, 11, 24), catchup=False,
         max_active_runs=1, tags=["dbt"]) as dag:
    # #10 (11-27): select 없는 build 는 거래소 대조 테스트(assert_*_reconcile_no_loss)까지 돌려 #7 공백이 있는 날마다 매시간 failed(11-25~27 57회) + Slack.
    # 매시간 할 일은 staging 과 1분 집계뿐. 대조 모델·테스트는 reconcile_* DAG, dq 모델은 dbt_daily_finalize 가 맡는다.
    BashOperator(task_id="dbt_build", pool="dbt", bash_command=dbt_cmd("build", select="staging int_upbit_orderbook_1m int_upbit_ohlcv_1m int_binance_ohlcv_1m"))
