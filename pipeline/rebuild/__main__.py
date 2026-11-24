"""rebuild-from-files (docs/01-design §5, §10 #3·#4).

11-24: 파이썬 파싱 → ClickHouse 가 파일(JSONL.gz·Parquet)을 file() 로 직접 읽어 target.envelopes 에 넣고, 라이브와 같은 MV(sql_parse) 가 raw_* 를 만든다.
파일은 ClickHouse 컨테이너에 /var/lib/clickhouse/user_files/<MP2_CH_FILE_PREFIX> 로 마운트돼 있어야 한다.

  python -m pipeline.rebuild --from 2026-11-23T23:40:00Z --to 2026-11-23T23:50:00Z [--target mp2_rebuild] [--streams upbit/trades,...] [--keep]

1) 닫힌 파일 조각을 읽어 target DB 의 raw_* 에 넣는다(멱등). 2) target 과 라이브(mp2) 를 셀 단위로 대조한다. 3) --keep 이 없으면 target 을 지운다.
종료 코드 0 = 전 셀 일치. 이것이 통과하기 전엔 다른 기능을 "완료" 라 하지 않는다.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from .. import chsink, filesink, schema
from .compare import compare

DDL_DIR = Path(__file__).resolve().parents[2] / "clickhouse" / "ddl"
DDL_FILES = ["001_raw.sql", "005_envelopes_mv.sql"]   # raw 표 + 봉투 Null 표 + 파싱 MV (재구성도 같은 파서를 지난다)


def _ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def ensure_target(client, target: str) -> None:
    for f in DDL_FILES:
        lines = [ln.split("  -- ")[0] if "  -- " in ln else ln for ln in (DDL_DIR / f).read_text().splitlines() if not ln.strip().startswith("--")]
        sql = "\n".join(lines).replace("mp2.", f"{target}.").replace("DATABASE IF NOT EXISTS mp2", f"DATABASE IF NOT EXISTS {target}")
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            client.command(stmt)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="t_from", required=True)
    ap.add_argument("--to", dest="t_to", required=True)
    ap.add_argument("--root", default=os.getenv("MP2_DATA_ROOT", "data/raw"))
    ap.add_argument("--live", default="mp2")
    ap.add_argument("--target", default="mp2_rebuild")
    ap.add_argument("--streams", default=",".join(f"{s}/{t}" for s, t in schema.TABLES))
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--late-slack-sec", type=int, default=3600,
                    help="이벤트 시각이 창 안인데 수신은 늦을 수 있다(접속 시 스냅샷·재전송). 창 끝 + 이만큼까지의 조각을 읽고 행은 이벤트 시각으로 거른다")
    a = ap.parse_args()
    t_from, t_to = _ts(a.t_from), _ts(a.t_to)
    f_ms, t_ms = int(t_from.timestamp() * 1000), int(t_to.timestamp() * 1000)

    # 열린 조각(.tmp)은 읽지 않으므로, 창 끝 + slack 이 "지금 - 회전 주기" 보다 최근이면 마지막 행들이 빠져 거짓 DIFF 가 난다.
    # 2026-11-24 00:01 에 23:54~00:00 창을 돌려 셀당 +1 이 난 것이 이 경우였다.
    newest_needed_ms = t_ms + a.late_slack_sec * 1000
    now_ms = int(time.time() * 1000)
    if newest_needed_ms > now_ms - filesink.ROTATE_SEC * 1000:
        wait_s = (newest_needed_ms - (now_ms - filesink.ROTATE_SEC * 1000)) / 1000
        print(f"WARNING: window end + slack is within the chunk rotation period ({filesink.ROTATE_SEC}s); "
              f"open chunks are excluded. Re-run in {wait_s:.0f}s for a definitive result.", file=sys.stderr)

    client = chsink.make_client(os.getenv("CH_HOST", "127.0.0.1"), int(os.getenv("CH_PORT", "8123")),
                                os.getenv("CH_USER", "default"), os.environ["CH_PASSWORD"], a.live)
    ensure_target(client, a.target)
    tclient = chsink.make_client(os.getenv("CH_HOST", "127.0.0.1"), int(os.getenv("CH_PORT", "8123")),
                                 os.getenv("CH_USER", "default"), os.environ["CH_PASSWORD"], a.target)
    ok = True
    print(f"rebuild window {t_from:%Y-%m-%d %H:%M:%S}Z ~ {t_to:%Y-%m-%d %H:%M:%S}Z  root={a.root}  live={a.live}  target={a.target}  engine=clickhouse(file→envelopes→MV)")
    print(f"{'stream':<18}{'files':>6}{'rows_in':>10}{'secs':>7} | {'cells':>6}{'diff':>6}  result")
    ch_prefix = os.getenv("MP2_CH_FILE_PREFIX", "mp2")
    data_root = Path(a.root).parent          # <data>/raw → <data>
    for key in [tuple(x.split("/")) for x in a.streams.split(",") if x]:
        t = schema.table_for(*key)
        t0 = time.monotonic()
        lo, hi = f_ms - a.late_slack_sec * 1000, t_ms + a.late_slack_sec * 1000
        paths = filesink.list_chunks(a.root, *key, lo, hi)
        n_in = 0
        for p in paths:
            rel = f"{ch_prefix}/{p.relative_to(data_root)}"
            if p.suffix == ".parquet":
                src = f"file('{rel}', Parquet)"
                recv_expr = "recv_ts"
            else:
                src = f"file('{rel}', JSONEachRow, 'source String, stream String, schema_version UInt8, recv_ts Int64, payload String')"
                recv_expr = "fromUnixTimestamp64Milli(recv_ts, 'UTC')"
            tclient.command(
                f"INSERT INTO {a.target}.envelopes SELECT source, stream, schema_version, {recv_expr} AS recv_ts, payload FROM {src} "
                f"WHERE toUnixTimestamp64Milli({recv_expr}) >= {lo} AND toUnixTimestamp64Milli({recv_expr}) < {hi}",
                settings={"input_format_json_read_objects_as_strings": 1, "max_threads": 2, "max_insert_threads": 1, "prefer_column_name_to_alias": 1, "precise_float_parsing": 1})
        # INSERT 는 행수를 돌려주지 않으므로 target 의 raw 행수로 센다 (MV 를 지난 뒤의 수)
        n_in = int(tclient.command(f"SELECT count() FROM {a.target}.{t.name}"))
        diffs, cells = compare(client, a.live, a.target, t, t_from, t_to)
        res = "OK" if not diffs else "DIFF"
        if diffs: ok = False
        print(f"{'/'.join(key):<18}{len(paths):>6}{n_in:>10}{time.monotonic() - t0:>7.1f} | {cells:>6}{len(diffs):>6}  {res}")
        for d in diffs[:20]:
            print(f"    {d.cell:<14} {d.hour}  live={d.left_n} rebuilt={d.right_n} hash_equal={d.hash_equal}")
    if not a.keep:
        client.command(f"DROP DATABASE IF EXISTS {a.target}")
    print("RESULT:", "ALL CELLS EQUAL" if ok else "MISMATCH")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
