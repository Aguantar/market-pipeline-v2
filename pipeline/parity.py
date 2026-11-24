"""파서 동치 테스트 (docs/01-design §10 #3): 같은 봉투 파일을 (a) 파이썬 parse.py 로, (b) ClickHouse sql_parse 로 파싱해 raw 테이블에 넣고
(cell_key, hour) 셀의 행수·cityHash64 를 비교한다. 둘이 같아야 "파서는 한 곳" 이라고 말할 수 있다.

  CH_PASSWORD=... python -m pipeline.parity --files 'raw/upbit/trades/dt=2026-11-24/hour=00/part-*.jsonl.gz' --stream upbit/trades
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from . import chsink, filesink, parse, schema, sql_parse
from .rebuild.__main__ import ensure_target
from .rebuild.compare import compare

ENV_SCHEMA = "source String, stream String, schema_version UInt8, recv_ts Int64, payload String"


def run(client_factory, data_root: Path, ch_prefix: str, key: tuple[str, str], rel_glob: str) -> dict:
    t = schema.table_for(*key)
    admin = client_factory("mp2")
    for db in ("mp2_parity_py", "mp2_parity_sql"):
        admin.command(f"DROP DATABASE IF EXISTS {db}")
        ensure_target(admin, db)
    # (a) python
    py = client_factory("mp2_parity_py")
    files = sorted(data_root.glob(rel_glob))
    b = chsink.CHBatcher(py, t, batch_rows=5000, batch_sec=1e9)
    n_py = 0
    t0 = time.monotonic()
    for env in filesink.iter_envelopes(files):
        p = parse.parse(env)
        if p:
            b.add(p[1]); n_py += 1
            if len(b.buf) >= 5000:
                b.flush()
    b.flush()
    py_s = time.monotonic() - t0
    assert b.pending_rows == 0, "python-path insert failed"
    # (b) sql
    t0 = time.monotonic()
    from_clause = (f"file('{ch_prefix}/{rel_glob}', JSONEachRow, '{ENV_SCHEMA}')")
    sel = sql_parse.select_sql(key, from_clause, recv=sql_parse.RECV_FROM_FILE)
    admin.command(f"INSERT INTO mp2_parity_sql.{t.name} ({', '.join(t.columns)}) {sel}",
                  settings={"input_format_json_read_objects_as_strings": 1, "max_threads": 2, "prefer_column_name_to_alias": 1, "precise_float_parsing": 1})
    sql_s = time.monotonic() - t0
    n_sql = int(admin.command(f"SELECT count() FROM mp2_parity_sql.{t.name}"))
    diffs, cells = compare(admin, "mp2_parity_py", "mp2_parity_sql", t, datetime(2000, 1, 1, tzinfo=timezone.utc), datetime(2100, 1, 1, tzinfo=timezone.utc))
    return {"stream": "/".join(key), "files": len(files), "rows_py": n_py, "rows_sql": n_sql, "cells": cells, "diff_cells": len(diffs),
            "py_s": round(py_s, 1), "sql_s": round(sql_s, 1), "diffs": diffs[:10]}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", default=os.getenv("MP2_DATA", "data"))
    ap.add_argument("--ch-prefix", default=os.getenv("MP2_CH_FILE_PREFIX", "mp2"))
    ap.add_argument("--day", default=None, help="해당 UTC 일자의 hour=00 조각 전부 (스트림 4개)")
    ap.add_argument("--streams", default=",".join(f"{s}/{t}" for s, t in schema.TABLES))
    ap.add_argument("--keep", action="store_true", help="비교 DB 를 남긴다(차이 조사용)")
    a = ap.parse_args()

    def cf(db):
        return chsink.make_client(os.getenv("CH_HOST", "127.0.0.1"), int(os.getenv("CH_PORT", "8123")), os.getenv("CH_USER", "default"), os.environ["CH_PASSWORD"], db)

    ok = True
    print(f"{'stream':<18}{'files':>6}{'rows_py':>9}{'rows_sql':>9}{'cells':>7}{'diff':>6}{'py_s':>7}{'sql_s':>7}  result")
    for key in [tuple(x.split("/")) for x in a.streams.split(",") if x]:
        rel = f"raw/{key[0]}/{key[1]}/dt={a.day}/hour=00/part-*.jsonl.gz"
        r = run(cf, Path(a.data_root), a.ch_prefix, key, rel)
        res = "OK" if r["diff_cells"] == 0 and r["rows_py"] == r["rows_sql"] else "DIFF"
        ok &= res == "OK"
        print(f"{r['stream']:<18}{r['files']:>6}{r['rows_py']:>9}{r['rows_sql']:>9}{r['cells']:>7}{r['diff_cells']:>6}{r['py_s']:>7}{r['sql_s']:>7}  {res}")
        for d in r["diffs"]:
            print("   ", d)
    if not a.keep:
        cf("mp2").command("DROP DATABASE IF EXISTS mp2_parity_py"); cf("mp2").command("DROP DATABASE IF EXISTS mp2_parity_sql")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
