"""Upbit 수집기: STREAM=trades | orderbook. KRW 전 마켓, 5분마다 마켓 목록 갱신(같은 연결에 구독 재전송 - v1 검증, docs/00 §2)."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import urllib.request
import uuid

from .base import Connection, Sink, main

log = logging.getLogger("collector")
WS_URL = "wss://api.upbit.com/websocket/v1"
MARKET_URL = "https://api.upbit.com/v1/market/all?is_details=false"
STREAM = os.getenv("STREAM", "trades")              # trades | orderbook
QUOTE = os.getenv("QUOTE_CURRENCY", "KRW")
ORDERBOOK_LEVEL = os.getenv("ORDERBOOK_LEVEL", "15")
MARKET_REFRESH_SEC = float(os.getenv("MARKET_REFRESH_SEC", "300"))
MARKETS_ENV = os.getenv("MARKETS", "")


def fetch_markets() -> list[str]:
    if MARKETS_ENV.strip():
        return [m for m in MARKETS_ENV.split(",") if m]
    with urllib.request.urlopen(MARKET_URL, timeout=15) as r:
        return sorted(m["market"] for m in json.load(r) if m["market"].startswith(QUOTE + "-"))


class UpbitConn(Connection):
    name = f"upbit-{STREAM}"

    def __init__(self, stop, sink: Sink, markets: list[str]):
        super().__init__(stop)
        self.sink, self.markets = sink, markets
        self.ty = "trade" if STREAM == "trades" else "orderbook"

    def url(self): return WS_URL

    def subscribe_messages(self):
        codes = self.markets if self.ty == "trade" else [f"{m}.{ORDERBOOK_LEVEL}" for m in self.markets]
        return [json.dumps([{"ticket": str(uuid.uuid4())[:8]}, {"type": self.ty, "codes": codes}, {"format": "SIMPLE"}])]

    def handle(self, raw, recv_ts):
        d = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
        if d.get("ty") != self.ty:
            return []            # 구독 응답·상태 메시지
        return [(self.sink, d)]

    def gap_sinks(self): return [self.sink]


async def build(stop, client):
    markets = await asyncio.get_running_loop().run_in_executor(None, fetch_markets)
    log.info("upbit %s: %d markets", STREAM, len(markets))
    sink = Sink("upbit", STREAM, client)
    conn = UpbitConn(stop, sink, markets)

    async def refresh():
        if MARKETS_ENV.strip():
            return
        new = await asyncio.get_running_loop().run_in_executor(None, fetch_markets)
        if new and new != conn.markets:
            added, removed = sorted(set(new) - set(conn.markets)), sorted(set(conn.markets) - set(new))
            log.info("market list changed +%s -%s -> resubscribe", added[:10], removed[:10])
            conn.markets = new
            conn.resubscribe.set()

    return [sink], [conn], [(MARKET_REFRESH_SEC, refresh)]


if __name__ == "__main__":
    main(build)
