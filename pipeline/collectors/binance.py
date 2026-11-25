"""Binance 수집기: STREAM=trades (USDT 전 심볼 @trade, 연결당 400 스트림) | depth (상위 N 심볼 @depth@100ms + 5분 REST 스냅샷).
심볼 목록은 1시간마다 갱신, 바뀌면 해당 연결 재접속(Binance 는 구독 교체보다 재접속이 단순 - v1 과 같음)."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import urllib.request

from .base import Connection, Sink, main
from .. import envelope

log = logging.getLogger("collector")
WS_BASE = os.getenv("BINANCE_WS_BASE", "wss://stream.binance.com:9443/stream?streams=")
REST = os.getenv("BINANCE_REST", "https://api.binance.com")
STREAM = os.getenv("STREAM", "trades")               # trades | depth
STREAMS_PER_CONN = int(os.getenv("STREAMS_PER_CONN", "400"))
DEPTH_TOP_N = int(os.getenv("DEPTH_TOP_N", "10"))
DEPTH_SNAPSHOT_SEC = float(os.getenv("DEPTH_SNAPSHOT_SEC", "300"))
DEPTH_SNAPSHOT_LIMIT = int(os.getenv("DEPTH_SNAPSHOT_LIMIT", "100"))
SYMBOL_REFRESH_SEC = float(os.getenv("SYMBOL_REFRESH_SEC", "3600"))
SYMBOLS_ENV = os.getenv("SYMBOLS", "")
RECONNECT_BEFORE_SEC = 23 * 3600                      # Binance 24h 연결 상한 전에 선제 재접속


def _get(url):
    with urllib.request.urlopen(url, timeout=20) as r:
        return json.load(r)


def fetch_symbols() -> list[str]:
    if SYMBOLS_ENV.strip():
        return [s for s in SYMBOLS_ENV.split(",") if s]
    info = _get(f"{REST}/api/v3/exchangeInfo?permissions=SPOT")
    return sorted(s["symbol"] for s in info["symbols"] if s["quoteAsset"] == "USDT" and s["status"] == "TRADING")


def fetch_top_symbols(n: int) -> list[str]:
    if SYMBOLS_ENV.strip():
        return [s for s in SYMBOLS_ENV.split(",") if s]
    t = [x for x in _get(f"{REST}/api/v3/ticker/24hr") if x["symbol"].endswith("USDT")]
    t.sort(key=lambda x: float(x["quoteVolume"]), reverse=True)
    return [x["symbol"] for x in t[:n]]


class BinanceConn(Connection):
    ping_interval = None       # Binance 서버가 ping 을 보낸다
    reconnect_on_resubscribe = True
    max_conn_sec = RECONNECT_BEFORE_SEC

    def __init__(self, stop, sink: Sink, idx: int, symbols: list[str]):
        super().__init__(stop)
        self.sink, self.idx, self.symbols = sink, idx, symbols
        self.name = f"binance-{STREAM}-{idx}"
        self.suffix = "@trade" if STREAM == "trades" else "@depth@100ms"

    def url(self): return WS_BASE + "/".join(s.lower() + self.suffix for s in self.symbols)

    def handle(self, raw, recv_ts):
        d = json.loads(raw).get("data")
        if not d or d.get("e") not in ("trade", "depthUpdate"):
            return []
        return [(self.sink, d)]

    def gap_sinks(self): return [self.sink]


def chunks(lst, n): return [lst[i:i + n] for i in range(0, len(lst), n)]


async def build(stop, client):
    loop = asyncio.get_running_loop()
    symbols = await loop.run_in_executor(None, (lambda: fetch_top_symbols(DEPTH_TOP_N)) if STREAM == "depth" else fetch_symbols)
    log.info("binance %s: %d symbols", STREAM, len(symbols))
    sink = Sink("binance", STREAM, client)
    conns = [BinanceConn(stop, sink, i, g) for i, g in enumerate(chunks(symbols, STREAMS_PER_CONN))]
    periodic = []

    async def refresh():
        new = await loop.run_in_executor(None, (lambda: fetch_top_symbols(DEPTH_TOP_N)) if STREAM == "depth" else fetch_symbols)
        cur = [s for c in conns for s in c.symbols]
        if new and set(new) != set(cur):   # 순서가 아니라 구성원이 바뀌었을 때만. 11-24 '+[] -[]' 재접속 4회가 순위 변동이었다
            log.info("symbol list changed +%s -%s -> reconnect", sorted(set(new) - set(cur))[:10], sorted(set(cur) - set(new))[:10])
            groups = chunks(new, STREAMS_PER_CONN)
            for i, c in enumerate(conns):
                if i < len(groups):
                    c.symbols = groups[i]
                    c.resubscribe.set()      # Binance: 재구독 대신 재접속이 필요 → run() 이 URL 을 다시 만들도록 끊는다
    periodic.append((SYMBOL_REFRESH_SEC, refresh))

    if STREAM == "depth":
        async def snapshots():
            for sym in [s for c in conns for s in c.symbols]:
                try:
                    snap = await loop.run_in_executor(None, _get, f"{REST}/api/v3/depth?symbol={sym}&limit={DEPTH_SNAPSHOT_LIMIT}")
                    snap.update({"e": "snapshot", "s": sym})
                    sink.emit(snap, envelope.now_ms())
                    await asyncio.sleep(0.25)
                except Exception as e:  # noqa: BLE001
                    log.warning("snapshot failed %s: %s", sym, e)
        periodic.append((DEPTH_SNAPSHOT_SEC, snapshots))
    return [sink], conns, periodic


if __name__ == "__main__":
    main(build)
