"""rebuild-from-files (docs/01-design §5).

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

from .. import chsink, filesink, parse, schema
from .compare import compare

DDL = Path(__file__).resolve().parents[2] / "clickhouse" / "ddl" / "001_raw.sql"


def _ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def ensure_target(client, target: str) -> None:
    lines = [ln for ln in DDL.read_text().splitlines() if not ln.strip().startswith("--")]
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
    print(f"rebuild window {t_from:%Y-%m-%d %H:%M:%S}Z ~ {t_to:%Y-%m-%d %H:%M:%S}Z  root={a.root}  live={a.live}  target={a.target}")
    print(f"{'stream':<18}{'files':>6}{'rows_read':>11}{'rows_parsed':>12}{'secs':>7} | {'cells':>6}{'diff':>6}  result")
    for key in [tuple(x.split("/")) for x in a.streams.split(",") if x]:
        t = schema.table_for(*key)
        t0 = time.monotonic()
        # 조각 선택과 봉투 읽기는 수신 시각으로 창 양쪽에 slack 을 둔다: 거래소 시각이 우리 시각보다 앞설 수 있고(시작 경계),
        # 접속 시 스냅샷·재전송은 늦게 온다(끝 경계). 창 판정 자체는 이벤트 시각으로만 한다.
        lo, hi = f_ms - a.late_slack_sec * 1000, t_ms + a.late_slack_sec * 1000
        paths = filesink.list_chunks(a.root, *key, lo, hi)
        b = chsink.CHBatcher(tclient, t, batch_rows=5000, batch_sec=1e9)
        n_read = n_parsed = 0
        ti = t.columns.index(t.time_col)
        for env in filesink.iter_envelopes(paths, lo, hi):
            n_read += 1
            p = parse.parse(env)
            if p and t_from <= p[1][ti] < t_to:      # 창 판정은 이벤트 시각 (라이브 집계와 같은 축)
                b.add(p[1]); n_parsed += 1
                if len(b.buf) >= 5000:
                    b.flush()
        b.flush()
        if b.pending_rows:
            print(f"{'/'.join(key):<18} INSERT FAILED pending={b.pending_rows}", file=sys.stderr); ok = False; continue
        diffs, cells = compare(client, a.live, a.target, t, t_from, t_to)
        res = "OK" if not diffs else "DIFF"
        if diffs: ok = False
        print(f"{'/'.join(key):<18}{len(paths):>6}{n_read:>11}{n_parsed:>12}{time.monotonic() - t0:>7.1f} | {cells:>6}{len(diffs):>6}  {res}")
        for d in diffs[:20]:
            print(f"    {d.cell:<14} {d.hour}  live={d.left_n} rebuilt={d.right_n} hash_equal={d.hash_equal}")
    if not a.keep:
        client.command(f"DROP DATABASE IF EXISTS {a.target}")
    print("RESULT:", "ALL CELLS EQUAL" if ok else "MISMATCH")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
