#!/usr/bin/env python3
"""v2 관찰 일일 요약 (v1 scripts/observe/daily_digest.py 의 v2 판, docs/01 §6 "일일 digest: v1 과 같은 형식 → 전/후 비교표").

읽기 전용 관측자: ~/pipeline-observation/mp2_metrics_5m.csv(5분 관측기) + ClickHouse 조회(gaps·incidents·reconcile·compaction_runs) + `airflow dags list-runs`.
파이프라인 경로는 건드리지 않는다. 출력: ~/pipeline-observation/mp2_digest_YYYY-MM-DD.md, docs/worklog.md 에 한 줄.
등록(호스트 cron, reconcile_upbit 06:35 UTC 뒤): 0 7 * * * /home/calme/market-pipeline-v2/.venv/bin/python /home/calme/market-pipeline-v2/scripts/observe/daily_digest.py
"""
from __future__ import annotations

import csv
import os
import statistics
import subprocess
import sys
from datetime import datetime, timedelta, timezone

REPO = "/home/calme/market-pipeline-v2"
CSV = os.path.expanduser("~/pipeline-observation/mp2_metrics_5m.csv")
OUT_DIR = os.path.expanduser("~/pipeline-observation")
WORKLOG = f"{REPO}/docs/worklog.md"
STREAMS = ["upbit/trades", "upbit/orderbook", "binance/trades", "binance/depth"]

# 합격선(docs/01 §5) + 임계 초안(docs/04 §2). 숫자를 바꾸면 여기와 docs/04 를 같이 고친다.
LAG_P95_WARN_MS = 3000          # A3: 3버킷 연속
FRESH_WARN_MS = 30000           # A2: 2버킷 연속
CH_MEM_WARN_MB = 2600           # A5
DISK_DAYS_WARN = 14             # A6
GAPS_PER_HOUR_WARN = 3          # A4


def fnum(v):
    try:
        return float(v)
    except Exception:  # noqa: BLE001
        return None


def pct(xs, p):
    xs = sorted(x for x in xs if x is not None)
    return xs[min(len(xs) - 1, int(len(xs) * p))] if xs else None


def ch(sql: str) -> list[list[str]]:
    pw = next(l.split("=", 1)[1].strip() for l in open(f"{REPO}/.env") if l.startswith("CLICKHOUSE_PASSWORD="))
    out = subprocess.run(["docker", "exec", "mp2-clickhouse", "clickhouse-client", "--password", pw, "-q", sql + " FORMAT TSV"],
                         capture_output=True, text=True, timeout=120)
    if out.returncode != 0:
        return [["ERROR", out.stderr.strip()[:200]]]
    return [ln.split("\t") for ln in out.stdout.splitlines() if ln]


def airflow_runs(dag: str, since: datetime) -> list[tuple[str, str]]:
    out = subprocess.run(["docker", "exec", "mp2-airflow-scheduler", "airflow", "dags", "list-runs", "-d", dag, "-o", "plain"],
                         capture_output=True, text=True, timeout=120)
    res = []
    for ln in out.stdout.splitlines()[1:]:
        parts = ln.split()
        if len(parts) >= 6 and parts[5] >= since.strftime("%Y-%m-%dT%H:%M:%S"):
            res.append((parts[1], parts[2]))
    return res


def main() -> int:
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=24)
    day = now.strftime("%Y-%m-%d")
    rows = [r for r in csv.DictReader(open(CSV)) if datetime.fromisoformat(r["ts"].replace("Z", "+00:00")) >= since]
    if not rows:
        sys.exit("no rows in last 24h")
    by_stream = {s: [r for r in rows if r["stream"] == s] for s in STREAMS}
    host = [r for r in rows if r["stream"] == "host"]

    # ── 합격선·임계 대조 ─────────────────────────────────────────
    checks = []
    for s in STREAMS:
        rs = by_stream[s]
        lag95 = [fnum(r["lag_p95_ms"]) for r in rs]
        fresh = [fnum(r["freshness_ms"]) for r in rs]
        zero = [r["ts"] for r in rs if fnum(r["rows_5m"]) == 0]
        streak = mx = 0
        for v in lag95:
            streak = streak + 1 if (v is not None and v >= LAG_P95_WARN_MS) else 0
            mx = max(mx, streak)
        fstreak = fmx = 0
        for v in fresh:
            fstreak = fstreak + 1 if (v is not None and v >= FRESH_WARN_MS) else 0
            fmx = max(fmx, fstreak)
        checks.append((f"A1 적재 0건 5분 버킷 ({s})", "위반" if zero else "정상", f"{len(zero)}개" + (f" 첫 {zero[0]}" if zero else "")))
        checks.append((f"A2 신선도 ≥{FRESH_WARN_MS//1000}s 2연속 ({s})", "위반" if fmx >= 2 else "정상", f"최대 연속 {fmx}, 24h max {pct(fresh, 1.0):.0f}ms"))
        checks.append((f"A3 lag p95 ≥{LAG_P95_WARN_MS//1000}s 3연속 ({s})", "위반" if mx >= 3 else "정상", f"최대 연속 {mx}, 24h p95-of-p95 {pct(lag95, 0.95):.0f}ms"))
    gaps = ch(f"SELECT source||'/'||stream, count(), round(sum(toUnixTimestamp64Milli(to_ts)-toUnixTimestamp64Milli(from_ts))/1000), max(c) FROM (SELECT source, stream, from_ts, to_ts, count() OVER (PARTITION BY source, stream, toStartOfHour(from_ts)) c FROM mp2.collector_gaps WHERE inserted_at >= now64() - INTERVAL 24 HOUR) GROUP BY 1 ORDER BY 1")
    for g in gaps:
        if g[0] == "ERROR":
            checks.append(("A4 재접속", "조회 실패", g[1])); break
        checks.append((f"A4 재접속 ≥{GAPS_PER_HOUR_WARN}/h ({g[0]})", "위반" if int(g[3]) >= GAPS_PER_HOUR_WARN else "정상", f"24h {g[1]}회 {g[2]}초, 1시간 최대 {g[3]}회"))
    chmem = [fnum(r["ch_mem_mb"]) for r in host]
    checks.append(("A5 ClickHouse RSS", "위반" if (pct(chmem, 1.0) or 0) >= CH_MEM_WARN_MB else "정상", f"max {pct(chmem, 1.0):.0f}MB p95 {pct(chmem, 0.95):.0f}MB"))
    raw_gb = [fnum(r["raw_dir_gb"]) for r in host if fnum(r["raw_dir_gb"])]
    disk_free = fnum(host[-1]["disk_free_gb"]) if host else None
    growth = (raw_gb[-1] - raw_gb[0]) if len(raw_gb) > 1 else None   # 24h 원문 증가(GB). Parquet·CH 는 compaction_runs/system.parts 에서 별도
    days_left = (disk_free / growth) if (growth and growth > 0 and disk_free) else None
    checks.append((f"A6 디스크 남은 일수 < {DISK_DAYS_WARN}", "위반" if (days_left is not None and days_left < DISK_DAYS_WARN) else "정상",
                   f"여유 {disk_free:.0f}GB, 원문 24h +{growth:.2f}GB → {days_left:.0f}일" if days_left else f"여유 {disk_free}GB"))

    # ── 일주기 결과 ───────────────────────────────────────────────
    dag_lines = []
    for d in ["export_columnar", "dbt_daily_finalize", "reconcile_binance", "backup_daily", "reconcile_upbit", "dbt_hourly", "health_check"]:
        runs = airflow_runs(d, since)
        n_ok = sum(1 for _, st in runs if st == "success"); n_bad = sum(1 for _, st in runs if st == "failed")
        dag_lines.append(f"| {d} | {len(runs)} | {n_ok} | {n_bad} |")
    recon = ch("SELECT 'upbit', count(), countIf(ratio_pct < 99), round(100*sum(ch_vol)/sum(candle_vol), 3) FROM mp2.int_upbit_reconcile_hourly WHERE toDate(hour_utc) = yesterday() AND candle_vol > 0 "
               "UNION ALL SELECT 'binance', count(), countIf(ratio_pct < 99), round(100*sum(ours_n)/sum(ex_n), 3) FROM mp2.int_binance_reconcile_hourly WHERE toDate(hour_utc) = yesterday() AND ex_n > 0")
    comp = ch("SELECT engine, stream, out_rows, round(out_bytes/1048576), verified, round(seconds), round(peak_mem_mb) FROM mp2.compaction_runs WHERE day = yesterday() ORDER BY run_at")
    inc = ch("SELECT severity, component, count() FROM mp2.pipeline_incidents WHERE detected_at >= now() - INTERVAL 24 HOUR GROUP BY 1, 2 ORDER BY 1, 2")

    # ── KST 시간대별 ─────────────────────────────────────────────
    kst = lambda r: (datetime.fromisoformat(r["ts"].replace("Z", "+00:00")) + timedelta(hours=9)).strftime("%H")  # noqa: E731
    hours = sorted({kst(r) for r in rows})
    hour_lines = []
    for h in hours:
        cells = []
        for s in STREAMS:
            rs = [r for r in by_stream[s] if kst(r) == h]
            cells.append(f"{pct([fnum(r['lag_p95_ms']) for r in rs], 1.0) or 0:.0f} / {statistics.mean([fnum(r['rows_5m']) or 0 for r in rs]) if rs else 0:.0f}")
        hour_lines.append(f"| {h} | " + " | ".join(cells) + " |")

    viol = sum(1 for _, v, _ in checks if v == "위반")
    md = [f"# v2 관찰 일일 요약 {day} (UTC {now:%H:%M}, 최근 24h, 5분 샘플 {len(host)}개)", "",
          f"위반 {viol} / 점검 {len(checks)}. 합격선 docs/01 §5, 임계 docs/04 §2(초안).", "",
          "| 점검 | 판정 | 근거 |", "|---|---|---|", *[f"| {a} | {b} | {c} |" for a, b, c in checks], "",
          "## 일주기 DAG (24h)", "", "| DAG | run | success | failed |", "|---|---|---|---|", *dag_lines, "",
          "## 거래소 대조 (어제)", "", "| 거래소 | 셀 | <99% | 가중 % |", "|---|---|---|---|",
          *[f"| {r[0]} | {r[1]} | {r[2]} | {r[3]} |" for r in recon], "",
          "## 컬럼형 export (어제, compaction_runs)", "", "| engine | stream | rows | MiB | verified | s | peak MB |", "|---|---|---|---|---|---|---|",
          *[f"| {' | '.join(r)} |" for r in comp], "",
          "## incidents (24h)", "", "| severity | component | n |", "|---|---|---|", *([f"| {' | '.join(r)} |" for r in inc] or ["| - | - | 0 |"]), "",
          "## KST 시간대별: lag p95 max(ms) / 5분 행수 평균", "",
          "| 시(KST) | " + " | ".join(STREAMS) + " |", "|---|" + "---|" * len(STREAMS), *hour_lines, ""]
    os.makedirs(OUT_DIR, exist_ok=True)
    out = f"{OUT_DIR}/mp2_digest_{day}.md"
    open(out, "w").write("\n".join(md))
    summary = (f"- {day} {now:%H:%M} UTC v2 관찰 일일 요약(자동): 샘플 {len(host)}, 위반 {viol}/{len(checks)}"
               + (" [" + ", ".join(a for a, v, _ in checks if v == "위반") + "]" if viol else "")
               + f", CH RSS max {pct(chmem, 1.0):.0f}MB, 디스크 여유 {disk_free:.0f}GB → {out}\n")
    open(WORKLOG, "a").write(summary)
    print(summary, end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
