import gzip
import os
import time
from pathlib import Path

from pipeline import envelope, filesink


def _env(ts, i):
    return envelope.make("upbit", "trades", {"i": i, "한글": "ok"}, recv_ts=ts)


def test_write_close_layout_and_roundtrip(tmp_path):
    w = filesink.ChunkWriter(tmp_path, "upbit", "trades")
    base = 1_795_132_800_000  # 2026-11-20 00:00:00 UTC
    for i in range(10):
        w.write(_env(base + i * 1000, i))
    assert w.rows == 10
    final = w.close()
    assert final is not None
    assert final.parent.parent.name == "dt=2026-11-20" and final.parent.name == "hour=00"
    assert final.name.startswith("part-1795132800000-0001") and final.suffix == ".gz"
    assert not list(final.parent.glob("*.tmp"))
    envs = list(filesink.iter_envelopes([final]))
    assert [e["payload"]["i"] for e in envs] == list(range(10))
    assert envs[0]["payload"]["한글"] == "ok" and envs[0]["source"] == "upbit"
    # 바이트 단위 재현성: mtime=0 고정이라 같은 내용은 같은 파일
    with gzip.open(final, "rb") as f:
        assert f.read().count(b"\n") == 10


def test_rotate_on_hour_boundary(tmp_path):
    w = filesink.ChunkWriter(tmp_path, "upbit", "orderbook")
    h0 = 1_795_132_800_000
    w.write(_env(h0 + 3_599_000, 1))
    w.write(_env(h0 + 3_600_000, 2))   # 01시 → 새 조각
    w.close()
    chunks = filesink.list_chunks(tmp_path, "upbit", "orderbook")
    assert [c.parent.name for c in chunks] == ["hour=00", "hour=01"]
    assert w.chunks_closed == 2


def test_rotate_on_size(tmp_path):
    w = filesink.ChunkWriter(tmp_path, "binance", "trades", max_bytes=2000)
    ts = 1_795_132_800_000
    for i in range(5000):
        w.write(envelope.make("binance", "trades", {"i": i, "pad": "x" * 50}, recv_ts=ts + i))
        if i % 500 == 0:
            w.flush()   # 압축 바이트가 파일에 반영되도록
    w.close()
    chunks = filesink.list_chunks(tmp_path, "binance", "trades")
    assert len(chunks) >= 2
    assert sum(1 for _ in filesink.iter_envelopes(chunks)) == 5000


def test_empty_chunk_is_not_written(tmp_path):
    w = filesink.ChunkWriter(tmp_path, "upbit", "trades")
    assert w.close() is None
    w.write(_env(1_795_132_800_000, 1))
    w.close()
    assert len(filesink.list_chunks(tmp_path, "upbit", "trades")) == 1


def test_list_chunks_time_filter(tmp_path):
    w = filesink.ChunkWriter(tmp_path, "upbit", "trades")
    h0 = 1_795_132_800_000
    for h in range(3):
        w.write(_env(h0 + h * 3_600_000 + 5, h))
    w.close()
    sel = filesink.list_chunks(tmp_path, "upbit", "trades", from_ts_ms=h0 + 3_600_000, to_ts_ms=h0 + 7_200_000)
    assert [c.parent.name for c in sel] == ["hour=01"]
    envs = list(filesink.iter_envelopes(sel, from_ts_ms=h0 + 3_600_000, to_ts_ms=h0 + 7_200_000))
    assert [e["payload"]["i"] for e in envs] == [1]


def test_recover_tmp_after_crash(tmp_path):
    """플러시된 줄까지는 살리고, 플러시 안 된 꼬리는 버린다."""
    w = filesink.ChunkWriter(tmp_path, "upbit", "trades", flush_sec=10**9)
    ts = 1_795_132_800_000
    for i in range(100):
        w.write(_env(ts + i, i))
    w.flush()                       # 100행 디스크 도달
    for i in range(100, 150):
        w.write(_env(ts + i, i))    # 50행은 gzip 버퍼에만
    # 크래시 흉내: 닫지 않고 핸들만 버린다
    w._fh.close()
    w._gz = w._fh = None
    tmps = list(tmp_path.glob("*/*/dt=*/hour=*/*.tmp"))
    assert len(tmps) == 1
    # 다른 스트림의 열린 조각은 건드리지 않는다 (2026-11-23 사고)
    other = filesink.ChunkWriter(tmp_path, "binance", "trades", flush_sec=10**9)
    other.write(envelope.make("binance", "trades", {"k": 1}, recv_ts=ts))
    assert filesink.recover_tmp(tmp_path, "upbit", "orderbook") == []
    assert len(list(tmp_path.glob("binance/trades/dt=*/hour=*/*.tmp"))) == 1
    rec = filesink.recover_tmp(tmp_path, "upbit", "trades")
    assert len(list(tmp_path.glob("binance/trades/dt=*/hour=*/*.tmp"))) == 1
    other.close()
    assert len(rec) == 1 and rec[0].name.endswith("-recovered.jsonl.gz")
    assert not list(tmp_path.glob("*/*/dt=*/hour=*/*.tmp"))
    got = [e["payload"]["i"] for e in filesink.iter_envelopes(rec)]
    assert got == list(range(100))
    # 닫힌 조각 목록에도 잡힌다
    assert rec[0] in filesink.list_chunks(tmp_path, "upbit", "trades")


def test_truncated_gzip_yields_complete_lines_only(tmp_path):
    p = tmp_path / "x.jsonl.gz"
    n = 2000
    with gzip.open(p, "wb") as f:
        for i in range(n):
            f.write(envelope.dumps(_env(1, i)))
    data = p.read_bytes()
    p.write_bytes(data[: len(data) // 2])   # 중간에서 잘림 (작은 파일은 deflate 블록 머리만 남아 0줄일 수 있어 2,000줄로)
    lines = list(filesink.iter_lines_gz(p))
    assert 0 < len(lines) < n
    for ln in lines:
        envelope.loads(ln)   # 전부 완전한 JSON


def test_parquet_chunks_are_listed_and_read(tmp_path):
    """compact.py 출력 형식의 Parquet 을 rebuild 가 같은 길로 읽는다."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    raw_root = tmp_path / "raw"
    w = filesink.ChunkWriter(raw_root, "upbit", "trades")
    h0 = 1_795_132_800_000
    w.write(_env(h0 + 5, 1)); w.close()                                   # 2026-11-20 JSONL
    pdir = tmp_path / "parquet" / "upbit" / "trades" / "dt=2026-11-19"   # 전날은 Parquet 로 압축돼 있음
    pdir.mkdir(parents=True)
    recv = h0 - 86_400_000 + 7
    tbl = pa.table({"source": ["upbit"], "stream": ["trades"], "schema_version": pa.array([1], pa.uint8()),
                    "recv_ts": pa.array([recv], pa.timestamp("ms", tz="UTC")), "payload": ['{"i": 99, "한글": "ok"}']})
    pq.write_table(tbl, pdir / "part-0000.parquet", compression="zstd")
    chunks = filesink.list_chunks(raw_root, "upbit", "trades")
    assert [c.suffix for c in chunks] == [".gz", ".parquet"]
    envs = list(filesink.iter_envelopes(chunks))
    assert [e["payload"]["i"] for e in envs] == [1, 99]
    assert envs[1]["recv_ts"] == recv and envs[1]["payload"]["한글"] == "ok"
    only_prev = filesink.list_chunks(raw_root, "upbit", "trades", from_ts_ms=h0 - 86_400_000, to_ts_ms=h0)
    assert [c.suffix for c in only_prev] == [".parquet"]
