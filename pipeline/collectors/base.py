"""수집기 공통 런타임 (docs/01-design §3).

- 받은 메시지를 봉투에 싸서 두 곳에 쓴다: 파일(ChunkWriter, 1순위) + ClickHouse(CHBatcher, 2순위).
- 무프레임 STALL_SEC(30초) → 연결을 죽은 것으로 보고 즉시 재연결, 백오프 상한 60초. 재연결마다 collector_gaps 1행.
  v1: 120초 감지·1135초 백오프로 03시 7.7% 유실 (docs/00 §5, 11-20).
- 파일 디스크 쓰기가 실패하면 프로세스를 멈춘다(원본 없는 적재는 하지 않는다). ClickHouse 실패는 배처가 재시도한다.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone

import websockets

from .. import chsink, envelope, filesink, parse, schema

log = logging.getLogger("collector")

STALL_SEC = float(os.getenv("STALL_SEC", "30"))
BACKOFF_MAX = float(os.getenv("BACKOFF_MAX_SEC", "60"))
STATS_SEC = float(os.getenv("STATS_SEC", "30"))
DATA_ROOT = os.getenv("MP2_DATA_ROOT", "/data/raw")


def ch_client():
    return chsink.make_client(os.getenv("CH_HOST", "127.0.0.1"), int(os.getenv("CH_PORT", "8123")),
                              os.getenv("CH_USER", "default"), os.environ["CH_PASSWORD"], os.getenv("CH_DB", "mp2"))


def _dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


class Sink:
    """(source, stream) 하나에 파일 기록기 + ClickHouse 배처."""

    def __init__(self, source: str, stream: str, client):
        self.source, self.stream = source, stream
        self.table: schema.RawTable = schema.table_for(source, stream)
        self.writer = filesink.ChunkWriter(DATA_ROOT, source, stream)
        self.batcher = chsink.CHBatcher(client, self.table)
        self.client = client
        self.recv = self.parsed = self.skipped = self.parse_err = 0
        self.lag_ms: list[int] = []

    def emit(self, payload: dict, recv_ts: int) -> None:
        env = envelope.make(self.source, self.stream, payload, recv_ts)
        self.writer.write(env)                 # 실패하면 예외 → 프로세스 종료 (의도)
        self.recv += 1
        try:
            parsed = parse.parse(env)
        except Exception as e:  # noqa: BLE001
            self.parse_err += 1
            if self.parse_err <= 5 or self.parse_err % 1000 == 0:
                log.warning("parse error (%d): %s payload=%s", self.parse_err, e, json.dumps(payload)[:200])
            return
        if parsed is None:
            self.skipped += 1
            return
        _, row = parsed
        self.batcher.add(row)
        self.parsed += 1
        ev = row[self.table.columns.index(self.table.time_col)]
        self.lag_ms.append(recv_ts - int(ev.timestamp() * 1000))

    async def tick(self) -> None:
        self.writer.tick()
        if self.batcher.due() or (self.batcher.pending and time.monotonic() >= self.batcher.next_retry):
            await self.batcher.flush_async()

    async def close(self) -> None:
        self.writer.close()
        await self.batcher.flush_async()

    def record_gap(self, conn: str, from_ms: int, to_ms: int, reason: str) -> None:
        try:
            self.client.insert("collector_gaps", [(self.source, self.stream, conn, _dt(from_ms), _dt(to_ms), reason)],
                               column_names=["source", "stream", "conn", "from_ts", "to_ts", "reason"])
        except Exception as e:  # noqa: BLE001
            log.warning("gap record failed: %s", e)

    def stats_line(self) -> str:
        lags = sorted(self.lag_ms)
        p50 = lags[len(lags) // 2] if lags else 0
        p95 = lags[int(len(lags) * 0.95)] if lags else 0
        self.lag_ms = []
        b = self.batcher.stats()
        return (f"[STATS] {self.source}/{self.stream} recv={self.recv} parsed={self.parsed} skipped={self.skipped} parse_err={self.parse_err} "
                f"files_closed={self.writer.chunks_closed} open_rows={self.writer.rows} | ch inserted={b['inserted']} pending={b['pending']} "
                f"dropped={b['dropped']} failed={b['failed_batches']} insert_p95={b['insert_p95_ms']}ms | lag_p50={p50}ms lag_p95={p95}ms")


class Connection:
    """웹소켓 연결 하나. 서브클래스가 url()/subscribe_messages()/handle(raw)->list[(sink, payload)] 를 준다."""

    name = "conn"
    ping_interval = 20
    ping_timeout = 10
    max_size = 2 ** 23
    reconnect_on_resubscribe = False   # True 면 구독 변경 시 재접속 (Binance: URL 에 스트림이 박혀 있음)
    max_conn_sec: float | None = None  # 선제 재접속 (Binance 24h 상한)

    def __init__(self, stop: asyncio.Event):
        self.stop = stop
        self.resubscribe = asyncio.Event()
        self.reconnects = 0
        self.last_frame_ms = 0
        self.frames = 0
        self._last_reason = "unknown"

    def url(self) -> str: raise NotImplementedError
    def subscribe_messages(self) -> list[str]: return []
    def handle(self, raw: str | bytes, recv_ts: int) -> list[tuple[Sink, dict]]: raise NotImplementedError
    def gap_sinks(self) -> list[Sink]: raise NotImplementedError

    async def run(self) -> None:
        backoff = 1.0
        while not self.stop.is_set():
            opened_ms = envelope.now_ms()
            reason = "unknown"
            try:
                async with websockets.connect(self.url(), ping_interval=self.ping_interval, ping_timeout=self.ping_timeout,
                                              max_size=self.max_size) as ws:
                    for m in self.subscribe_messages():
                        await ws.send(m)
                    log.info("%s connected", self.name)
                    if self.reconnects and self.last_frame_ms:
                        now = envelope.now_ms()
                        for s in self.gap_sinks():
                            s.record_gap(self.name, self.last_frame_ms, now, self._last_reason)
                    backoff = 1.0
                    opened_mono = time.monotonic()
                    while not self.stop.is_set():
                        if self.resubscribe.is_set():
                            self.resubscribe.clear()
                            if self.reconnect_on_resubscribe:
                                reason = "resubscribe"
                                break
                            for m in self.subscribe_messages():
                                await ws.send(m)
                            log.info("%s resubscribed", self.name)
                        if self.max_conn_sec and time.monotonic() - opened_mono >= self.max_conn_sec:
                            reason = "planned-reconnect"
                            log.info("%s planned reconnect", self.name)
                            break
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=STALL_SEC)
                        except asyncio.TimeoutError:
                            reason = f"stall>{int(STALL_SEC)}s"
                            log.warning("%s no frame for %.0fs - reconnecting", self.name, STALL_SEC)
                            break
                        recv_ts = envelope.now_ms()
                        self.last_frame_ms = recv_ts
                        self.frames += 1
                        for sink, payload in self.handle(raw, recv_ts):
                            sink.emit(payload, recv_ts)
                    else:
                        reason = "shutdown"
            except Exception as e:  # noqa: BLE001
                reason = f"{type(e).__name__}: {str(e)[:80]}"
                log.warning("%s error: %s - retry in %.0fs", self.name, reason, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, BACKOFF_MAX)
            if self.stop.is_set():
                return
            self._last_reason = reason
            self.reconnects += 1


async def run_collector(sinks: list[Sink], conns: list[Connection], stop: asyncio.Event, periodic=None) -> None:
    """연결들을 돌리고, 0.5초마다 싱크 tick, STATS_SEC 마다 통계. periodic: (interval_sec, coroutine_fn) 목록(마켓 갱신 등)."""
    recovered = filesink.recover_tmp(DATA_ROOT)
    if recovered:
        log.info("recovered %d tmp chunk(s) from previous run", len(recovered))
    tasks = [asyncio.create_task(c.run()) for c in conns]
    last_stats = time.monotonic()
    last_periodic = {i: time.monotonic() for i in range(len(periodic or []))}
    try:
        while not stop.is_set():
            await asyncio.sleep(0.5)
            for s in sinks:
                await s.tick()
            now = time.monotonic()
            for i, (interval, fn) in enumerate(periodic or []):
                if now - last_periodic[i] >= interval:
                    last_periodic[i] = now
                    try:
                        await fn()
                    except Exception as e:  # noqa: BLE001
                        log.warning("periodic task failed: %s", e)
            if now - last_stats >= STATS_SEC:
                last_stats = now
                for s in sinks:
                    log.info(s.stats_line() + f" reconnects={sum(c.reconnects for c in conns)} conns={len(conns)}")
    finally:
        log.info("stopping: closing files, flushing clickhouse")
        for t in tasks:
            t.cancel()
        for s in sinks:
            await s.close()
        for s in sinks:
            log.info(s.stats_line())


def main(build):
    """build(stop, client) -> (sinks, conns, periodic). 시그널 처리와 로깅은 여기서."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    async def _run():
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        client = ch_client()
        sinks, conns, periodic = await build(stop, client)
        await run_collector(sinks, conns, stop, periodic)

    asyncio.run(_run())
