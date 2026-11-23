"""불변 원본 파일 (docs/01-design §3).

레이아웃: <root>/<source>/<stream>/dt=YYYY-MM-DD/hour=HH/part-<first_recv_ts_ms>-<seq>.jsonl.gz
- 열린 조각은 `.tmp` 접미사. 닫을 때 gzip 마감 → fsync → rename. 닫힌 조각만 재처리 대상.
- 회전: 시간 경계(UTC, recv_ts 기준) / ROTATE_SEC(기본 300초) / MAX_BYTES(압축 기준). 정전 때 잃는 최대치는 열린 조각 하나.
- 열린 조각도 FLUSH_SEC 마다 gzip 동기 플러시 + fsync 하므로, 비정상 종료 뒤 `recover_tmp()` 가 읽을 수 있는 줄까지 살려 `-recovered` 조각으로 닫는다.
  v1 교훈: fsync 없는 쓰기는 정전에서 0B 파일을 남겼다 (docs/00 §5, 11-20).
"""
from __future__ import annotations

import gzip
import io
import os
import time
import zlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from . import envelope

ROTATE_SEC = 300
MAX_BYTES = 256 * 1024 * 1024
FLUSH_SEC = 5
GZIP_LEVEL = 6


def _hour_key(recv_ts_ms: int) -> tuple[str, str]:
    d = datetime.fromtimestamp(recv_ts_ms / 1000, tz=timezone.utc)
    return d.strftime("%Y-%m-%d"), d.strftime("%H")


class ChunkWriter:
    def __init__(self, root: str | Path, source: str, stream: str,
                 rotate_sec: int = ROTATE_SEC, max_bytes: int = MAX_BYTES, flush_sec: int = FLUSH_SEC):
        self.root = Path(root) / source / stream
        self.rotate_sec, self.max_bytes, self.flush_sec = rotate_sec, max_bytes, flush_sec
        self._fh: io.BufferedWriter | None = None
        self._gz: gzip.GzipFile | None = None
        self._tmp: Path | None = None
        self._final: Path | None = None
        self._opened_at = 0.0
        self._last_flush = 0.0
        self._hour: tuple[str, str] | None = None
        self._seq = 0
        self.rows = 0            # 현재 조각 행 수
        self.total_rows = 0
        self.chunks_closed = 0

    # ---- 조각 열기/닫기 ----
    def _open(self, recv_ts: int) -> None:
        dt, hh = _hour_key(recv_ts)
        d = self.root / f"dt={dt}" / f"hour={hh}"
        d.mkdir(parents=True, exist_ok=True)
        self._seq += 1
        name = f"part-{recv_ts}-{self._seq:04d}.jsonl.gz"
        self._final = d / name
        self._tmp = d / (name + ".tmp")
        self._fh = open(self._tmp, "wb")
        self._gz = gzip.GzipFile(fileobj=self._fh, mode="wb", compresslevel=GZIP_LEVEL, mtime=0)
        self._hour = (dt, hh)
        self._opened_at = self._last_flush = time.monotonic()
        self.rows = 0

    def _needs_rotate(self, recv_ts: int) -> bool:
        if self._gz is None:
            return False
        if _hour_key(recv_ts) != self._hour:
            return True
        if time.monotonic() - self._opened_at >= self.rotate_sec:
            return True
        return self._fh.tell() >= self.max_bytes

    def close(self) -> Path | None:
        """열린 조각을 마감한다. 닫힌 파일 경로를 돌려준다(행이 0이면 파일을 지우고 None)."""
        if self._gz is None:
            return None
        self._gz.close()
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._fh.close()
        final, tmp, rows = self._final, self._tmp, self.rows
        self._gz = self._fh = self._tmp = self._final = None
        if rows == 0:
            tmp.unlink(missing_ok=True)
            return None
        os.replace(tmp, final)
        _fsync_dir(final.parent)
        self.chunks_closed += 1
        return final

    # ---- 쓰기 ----
    def write(self, env: dict) -> None:
        recv_ts = env["recv_ts"]
        if self._needs_rotate(recv_ts):
            self.close()
        if self._gz is None:
            self._open(recv_ts)
        self._gz.write(envelope.dumps(env))
        self.rows += 1
        self.total_rows += 1
        now = time.monotonic()
        if now - self._last_flush >= self.flush_sec:
            self.flush()

    def flush(self) -> None:
        """열린 조각을 디스크까지 내린다. 비정상 종료 뒤 recover_tmp 가 여기까지는 읽는다."""
        if self._gz is None:
            return
        self._gz.flush(zlib.Z_SYNC_FLUSH)
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._last_flush = time.monotonic()

    def tick(self) -> None:
        """데이터가 없어도 주기 회전·플러시가 되도록 주기적으로 부른다."""
        if self._gz is None:
            return
        now = time.monotonic()
        if now - self._opened_at >= self.rotate_sec:
            self.close()
        elif now - self._last_flush >= self.flush_sec:
            self.flush()


def _fsync_dir(d: Path) -> None:
    fd = os.open(d, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


# ---- 읽기 ----
def iter_lines_gz(path: Path) -> Iterator[bytes]:
    """gzip 멤버가 잘려 있어도(비정상 종료) 읽을 수 있는 완전한 줄까지 돌려준다."""
    with open(path, "rb") as fh:
        d = zlib.decompressobj(16 + zlib.MAX_WBITS)
        buf = b""
        while True:
            chunk = fh.read(1 << 20)
            if not chunk:
                break
            try:
                buf += d.decompress(chunk)
            except zlib.error:
                break
            while True:
                i = buf.find(b"\n")
                if i < 0:
                    break
                yield buf[: i + 1]
                buf = buf[i + 1:]
        try:
            buf += d.flush()
        except zlib.error:
            pass
        while True:
            i = buf.find(b"\n")
            if i < 0:
                break
            yield buf[: i + 1]
            buf = buf[i + 1:]
        # 개행 없는 꼬리는 불완전한 줄 → 버린다


def list_chunks(root: str | Path, source: str, stream: str,
                from_ts_ms: int | None = None, to_ts_ms: int | None = None) -> list[Path]:
    """닫힌 조각만. 시간 범위는 시간 디렉터리 단위로 거른다(조각 안 recv_ts 는 읽는 쪽이 다시 거른다)."""
    base = Path(root) / source / stream
    out: list[Path] = []
    if not base.exists():
        return out
    for p in sorted(base.glob("dt=*/hour=*/part-*.jsonl.gz")):
        if from_ts_ms is not None or to_ts_ms is not None:
            dt = p.parent.parent.name[3:]
            hh = p.parent.name[5:]
            start = int(datetime.strptime(f"{dt} {hh}", "%Y-%m-%d %H").replace(tzinfo=timezone.utc).timestamp() * 1000)
            end = start + 3_600_000
            if to_ts_ms is not None and start >= to_ts_ms:
                continue
            if from_ts_ms is not None and end <= from_ts_ms:
                continue
        out.append(p)
    return out


def iter_envelopes(paths: list[Path], from_ts_ms: int | None = None, to_ts_ms: int | None = None) -> Iterator[dict]:
    for p in paths:
        for line in iter_lines_gz(p):
            env = envelope.loads(line)
            ts = env["recv_ts"]
            if from_ts_ms is not None and ts < from_ts_ms:
                continue
            if to_ts_ms is not None and ts >= to_ts_ms:
                continue
            yield env


def recover_tmp(root: str | Path) -> list[Path]:
    """비정상 종료가 남긴 .tmp 조각을 살려 닫는다. 읽을 수 있는 완전한 줄만 새 gzip 으로 다시 써 `-recovered` 이름으로 rename."""
    recovered: list[Path] = []
    for tmp in sorted(Path(root).glob("*/*/dt=*/hour=*/part-*.jsonl.gz.tmp")):
        lines = list(iter_lines_gz(tmp))
        if not lines:
            tmp.unlink()
            continue
        final = tmp.with_name(tmp.name[: -len(".jsonl.gz.tmp")] + "-recovered.jsonl.gz")
        with open(final.with_suffix(".gz.rtmp"), "wb") as fh:
            with gzip.GzipFile(fileobj=fh, mode="wb", compresslevel=GZIP_LEVEL, mtime=0) as gz:
                for ln in lines:
                    gz.write(ln)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(final.with_suffix(".gz.rtmp"), final)
        tmp.unlink()
        _fsync_dir(final.parent)
        recovered.append(final)
    return recovered
