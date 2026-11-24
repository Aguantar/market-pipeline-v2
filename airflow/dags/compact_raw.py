"""00:20 UTC, 대상일 = ds(어제): 스트림별 JSONL.gz → Parquet(zstd) 압축·검증·원본 삭제. 측정은 mp2.compaction_runs (2주차 Spark 비교 기준선)."""
from datetime import date, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from mp2_common import DEFAULT_ARGS

STREAMS = [("upbit", "trades"), ("upbit", "orderbook"), ("binance", "trades"), ("binance", "depth")]


def compact(source: str, stream: str, **context) -> dict:
    import os
    from pipeline import chsink, compact as cp
    day = date.fromisoformat(context["params"].get("target_date") or context["ds"])
    client = chsink.make_client(os.environ["CH_HOST"], 8123, "default", os.environ["CH_PASSWORD"], "mp2")
    res = cp.compact_day(client, source, stream, day, os.environ.get("MP2_DATA", "/data"), os.environ.get("MP2_CH_FILE_PREFIX", "mp2"))
    context["ti"].log.info("%s", res)
    return res


with DAG("compact_raw", default_args=DEFAULT_ARGS, schedule="20 0 * * *", start_date=datetime(2026, 11, 24), catchup=False,
         max_active_runs=1, tags=["storage"], params={"target_date": ""}) as dag:
    for s, t in STREAMS:
        PythonOperator(task_id=f"compact_{s}_{t}", python_callable=compact, op_kwargs={"source": s, "stream": t}, pool="dbt")
