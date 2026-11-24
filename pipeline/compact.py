"""compact_raw: 하루치 JSONL.gz 조각 → Parquet(zstd) 1파일, 검증 뒤 원본 삭제, 측정 기록 (docs/01-design §6, 디스크 7.9GB/일 실측으로 1주차에 앞당김).

엔진은 ClickHouse 자신이다: file('mp2/raw/...jsonl.gz', JSONEachRow) 를 읽어 file('mp2/parquet/...', Parquet) 로 쓴다. 추가 부품 0.
2주차에 같은 일을 Spark 로 만들어 compaction_runs 에 engine='spark' 로 나란히 적는다.

검증: 입력 (count, sum(cityHash64(payload, recv_ts))) == 출력. 같아야만 원본을 지운다. 다르면 둘 다 남기고 예외.
파일 레이아웃은 봉투 그대로: source, stream, schema_version, recv_ts(DateTime64(3)), payload(String=원문 JSON). rebuild 는 이 Parquet 도 읽는다.
"""
from __future__ import annotations

import logging
import os
import time
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

log = logging.getLogger("compact")

ENVELOPE_SCHEMA = "source String, stream String, schema_version UInt8, recv_ts Int64, payload String"
INPUT_SETTINGS = {"input_format_json_read_objects_as_strings": 1, "max_threads": 2, "max_memory_usage": 1_500_000_000}
OUTPUT_SETTINGS = {"output_format_parquet_compression_method": "zstd", "output_format_parquet_row_group_size": 200_000,
                   "output_format_parquet_string_as_string": 1}


def _fsync_dir(d: Path) -> None:
    fd = os.open(d, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def compact_day(client, source: str, stream: str, day: date, data_root: str | Path, ch_prefix: str, delete_source: bool = True) -> dict:
    """data_root: 호스트/컨테이너에서 보이는 data 디렉터리(raw/, parquet/ 포함). ch_prefix: ClickHouse user_files 아래 같은 디렉터리 이름."""
    data_root = Path(data_root)
    d = day.isoformat()
    src_dir = data_root / "raw" / source / stream / f"dt={d}"
    src_files = sorted(src_dir.glob("hour=*/part-*.jsonl.gz"))
    if not src_files:
        return {"source": source, "stream": stream, "day": d, "skipped": "no closed chunks"}
    tmps = list(src_dir.glob("hour=*/*.tmp"))
    if tmps:
        raise RuntimeError(f"{source}/{stream} {d}: {len(tmps)} open .tmp chunk(s) - collector still writing or needs recover_tmp; refusing")
    out_dir = data_root / "parquet" / source / stream / f"dt={d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(out_dir, 0o777)                     # ClickHouse(uid 101) 가 쓴다
    out_file = out_dir / "part-0000.parquet"
    if out_file.exists():
        out_file.unlink()                        # 재실행 멱등
    src_glob = f"{ch_prefix}/raw/{source}/{stream}/dt={d}/hour=*/part-*.jsonl.gz"
    out_rel = f"{ch_prefix}/parquet/{source}/{stream}/dt={d}/part-0000.parquet"
    src_bytes = sum(p.stat().st_size for p in src_files)

    t0 = time.monotonic()
    src = client.query(f"SELECT count(), sum(cityHash64(payload, recv_ts)) FROM file('{src_glob}', JSONEachRow, '{ENVELOPE_SCHEMA}')",
                       settings=INPUT_SETTINGS).result_rows[0]
    qid = f"compact-{source}-{stream}-{d}-{uuid.uuid4().hex[:8]}"
    client.command(
        f"INSERT INTO FUNCTION file('{out_rel}', Parquet) "
        f"SELECT source, stream, schema_version, fromUnixTimestamp64Milli(recv_ts) AS recv_ts, payload "
        f"FROM file('{src_glob}', JSONEachRow, '{ENVELOPE_SCHEMA}') ORDER BY recv_ts",
        settings={**INPUT_SETTINGS, **OUTPUT_SETTINGS, "query_id": qid})
    out = client.query(f"SELECT count(), sum(cityHash64(payload, toUnixTimestamp64Milli(recv_ts))) FROM file('{out_rel}', Parquet)",
                       settings={"max_threads": 2}).result_rows[0]
    seconds = time.monotonic() - t0
    verified = (int(src[0]) == int(out[0])) and (int(src[1]) == int(out[1]))
    out_bytes = out_file.stat().st_size if out_file.exists() else 0

    peak_mb = 0.0
    try:
        client.command("SYSTEM FLUSH LOGS")
        r = client.query(f"SELECT max(memory_usage) FROM system.query_log WHERE query_id = '{qid}' AND type = 'QueryFinish'").result_rows
        peak_mb = round((r[0][0] or 0) / 2**20, 1) if r else 0.0
    except Exception as e:  # noqa: BLE001
        log.warning("peak memory lookup failed: %s", e)

    res = {"source": source, "stream": stream, "day": d, "src_files": len(src_files), "src_rows": int(src[0]), "src_bytes": src_bytes,
           "out_rows": int(out[0]), "out_bytes": out_bytes, "verified": verified, "seconds": round(seconds, 1), "peak_mem_mb": peak_mb,
           "ratio": round(src_bytes / out_bytes, 2) if out_bytes else None}
    client.insert("compaction_runs",
                  [(datetime.now(timezone.utc).replace(tzinfo=None), "clickhouse", source, stream, day, len(src_files), int(src[0]), src_bytes,
                    int(out[0]), out_bytes, 1 if verified else 0, seconds, peak_mb, "" if verified else f"MISMATCH src={src} out={out}")],
                  column_names=["run_at", "engine", "source", "stream", "day", "src_files", "src_rows", "src_bytes", "out_rows", "out_bytes",
                                "verified", "seconds", "peak_mem_mb", "note"])
    if not verified:
        raise RuntimeError(f"{source}/{stream} {d}: verification failed src={src} out={out} - both kept")
    if delete_source:
        for p in src_files:
            p.unlink()
        for hd in sorted({p.parent for p in src_files}):
            _fsync_dir(hd)
            try:
                hd.rmdir()
            except OSError:
                pass
        res["deleted_files"] = len(src_files)
    log.info("compacted %s", res)
    return res


def main() -> int:
    import argparse
    import sys
    from . import chsink, schema
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", required=True)
    ap.add_argument("--streams", default=",".join(f"{s}/{t}" for s, t in schema.TABLES))
    ap.add_argument("--data-root", default=os.getenv("MP2_DATA", "data"))
    ap.add_argument("--ch-prefix", default=os.getenv("MP2_CH_FILE_PREFIX", "mp2"))
    ap.add_argument("--keep-source", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    client = chsink.make_client(os.getenv("CH_HOST", "127.0.0.1"), int(os.getenv("CH_PORT", "8123")), os.getenv("CH_USER", "default"),
                                os.environ["CH_PASSWORD"], "mp2")
    ok = True
    for key in [tuple(x.split("/")) for x in a.streams.split(",") if x]:
        try:
            r = compact_day(client, key[0], key[1], date.fromisoformat(a.day), a.data_root, a.ch_prefix, delete_source=not a.keep_source)
            print(r)
        except Exception as e:  # noqa: BLE001
            ok = False
            print(f"{key}: FAILED {e}", file=sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
