"""columnar export (docs/01-design §10 #2·#4): 하루치 봉투 파일(JSONL.gz) → ClickHouse 가 sql_parse 로 파싱 → 컬럼형 Parquet(zstd) 1파일/테이블.

- 원본 봉투 JSONL.gz 는 지우지 않는다(미니PC 단일 사본 영구, §10 #1). 오프사이트로 가는 것은 이 컬럼형 Parquet 다.
- 검증 = **그날의 hour 단위 셀을 라이브 raw 테이블(FINAL)과 대조**. 파일에서 파싱한 결과가 라이브와 같다는 "하루치 rebuild 증명" 을 매일 한다 (§10 #4).
- 측정은 compaction_runs (engine='clickhouse'). 2주차 Spark 가 같은 입력으로 같은 출력을 만들 때 나란히 적는다.
"""
from __future__ import annotations

import logging
import os
import time
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

from . import schema, sql_parse
from .rebuild.compare import _cells

log = logging.getLogger("compact")

ENV_SCHEMA = "source String, stream String, schema_version UInt8, recv_ts Int64, payload String"
SETTINGS = {"input_format_json_read_objects_as_strings": 1, "max_threads": 2, "max_insert_threads": 1, "max_memory_usage": 1_500_000_000,
            "prefer_column_name_to_alias": 1, "precise_float_parsing": 1,   # `... AS recv_ts` 별칭이 같은 SELECT 안의 recv_ts 컬럼 참조를 가리는 것을 막는다
            "output_format_parquet_compression_method": "zstd", "output_format_parquet_row_group_size": 200_000}


def export_day(client, source: str, stream: str, day: date, data_root: str | Path, ch_prefix: str, live_db: str = "mp2") -> dict:
    t = schema.table_for(source, stream)
    data_root = Path(data_root)
    d = day.isoformat()
    src_dir = data_root / "raw" / source / stream / f"dt={d}"
    src_files = sorted(src_dir.glob("hour=*/part-*.jsonl.gz"))
    if not src_files:
        return {"source": source, "stream": stream, "day": d, "skipped": "no closed chunks"}
    if list(src_dir.glob("hour=*/*.tmp")):
        raise RuntimeError(f"{source}/{stream} {d}: open .tmp chunk(s) present - refusing")
    out_dir = data_root / "parquet" / t.name / f"dt={d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(out_dir, 0o777)
    out_file = out_dir / "part-0000.parquet"
    if out_file.exists():
        out_file.unlink()
    src_glob = f"{ch_prefix}/raw/{source}/{stream}/dt={d}/hour=*/part-*.jsonl.gz"
    out_rel = f"{ch_prefix}/parquet/{t.name}/dt={d}/part-0000.parquet"
    src_bytes = sum(p.stat().st_size for p in src_files)
    day_from = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    day_to = datetime.fromtimestamp(day_from.timestamp() + 86400, tz=timezone.utc)

    sel = sql_parse.select_sql((source, stream), f"file('{src_glob}', JSONEachRow, '{ENV_SCHEMA}')", recv=sql_parse.RECV_FROM_FILE)
    # 그날의 이벤트 시각 행만(접속 스냅샷으로 전날 이벤트가 섞여 들어오므로) — 라이브 대조 창과 같은 기준
    sel_day = f"SELECT * FROM ({sel}) WHERE {t.time_col} >= toDateTime64('{day_from:%Y-%m-%d %H:%M:%S}', 3, 'UTC') AND {t.time_col} < toDateTime64('{day_to:%Y-%m-%d %H:%M:%S}', 3, 'UTC')"
    qid = f"export-{t.name}-{d}-{uuid.uuid4().hex[:8]}"
    t0 = time.monotonic()
    client.command(f"INSERT INTO FUNCTION file('{out_rel}', Parquet) {sel_day}", settings={**SETTINGS, "query_id": qid})
    # 검증: 출력 Parquet 의 셀 vs 라이브 raw FINAL 의 셀 (행수·cityHash64)
    hcols = ", ".join(t.hash_cols)
    out_cells = {(r[0], r[1]): (int(r[2]), int(r[3])) for r in client.query(
        f"SELECT {t.cell_key}, toString(toStartOfHour({t.time_col})), count(), sum(cityHash64({hcols})) FROM file('{out_rel}', Parquet) GROUP BY 1, 2",
        settings={"max_threads": 2}).result_rows}
    live_cells = _cells(client, live_db, t, day_from, day_to)
    diff = [k for k in set(out_cells) | set(live_cells) if out_cells.get(k, (0, 0)) != live_cells.get(k, (0, 0))]
    seconds = time.monotonic() - t0
    out_rows = sum(v[0] for v in out_cells.values())
    out_bytes = out_file.stat().st_size if out_file.exists() else 0
    peak_mb = 0.0
    try:
        client.command("SYSTEM FLUSH LOGS")
        r = client.query(f"SELECT max(memory_usage), sum(read_bytes) FROM system.query_log WHERE query_id = '{qid}' AND type = 'QueryFinish'").result_rows
        peak_mb = round((r[0][0] or 0) / 2**20, 1) if r else 0.0
    except Exception as e:  # noqa: BLE001
        log.warning("peak memory lookup failed: %s", e)
    verified = len(diff) == 0 and out_rows > 0
    res = {"source": source, "stream": stream, "day": d, "src_files": len(src_files), "src_bytes": src_bytes, "out_rows": out_rows, "out_bytes": out_bytes,
           "cells": len(set(out_cells) | set(live_cells)), "diff_cells": len(diff), "verified": verified, "seconds": round(seconds, 1), "peak_mem_mb": peak_mb,
           "ratio": round(src_bytes / out_bytes, 2) if out_bytes else None}
    client.insert("compaction_runs",
                  [(datetime.now(timezone.utc).replace(tzinfo=None), "clickhouse", source, stream, day, len(src_files), out_rows, src_bytes,
                    out_rows, out_bytes, 1 if verified else 0, seconds, peak_mb,
                    "columnar; verified vs live cells" if verified else f"columnar; DIFF cells={len(diff)} e.g. {sorted(diff)[:3]}")],
                  column_names=["run_at", "engine", "source", "stream", "day", "src_files", "src_rows", "src_bytes", "out_rows", "out_bytes",
                                "verified", "seconds", "peak_mem_mb", "note"])
    if not verified:
        raise RuntimeError(f"{t.name} {d}: file-parsed cells != live cells ({len(diff)} of {res['cells']}), e.g. {sorted(diff)[:5]}")
    log.info("exported %s", res)
    return res


def main() -> int:
    import argparse
    import sys
    from . import chsink
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", required=True)
    ap.add_argument("--streams", default=",".join(f"{s}/{t}" for s, t in schema.TABLES))
    ap.add_argument("--data-root", default=os.getenv("MP2_DATA", "data"))
    ap.add_argument("--ch-prefix", default=os.getenv("MP2_CH_FILE_PREFIX", "mp2"))
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    client = chsink.make_client(os.getenv("CH_HOST", "127.0.0.1"), int(os.getenv("CH_PORT", "8123")), os.getenv("CH_USER", "default"), os.environ["CH_PASSWORD"], "mp2")
    ok = True
    for key in [tuple(x.split("/")) for x in a.streams.split(",") if x]:
        try:
            print(export_day(client, key[0], key[1], date.fromisoformat(a.day), a.data_root, a.ch_prefix))
        except Exception as e:  # noqa: BLE001
            ok = False
            print(f"{key}: FAILED {e}", file=sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
