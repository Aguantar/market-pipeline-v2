"""메시지 봉투 (docs/01-design §3). 수집기는 파싱하지 않는다 - 거래소 원문을 payload 에 그대로 담아 두 곳(파일, ClickHouse)에 쓴다.

봉투가 계약이다: 소스·스트림·schema_version·수신시각 네 개만 약속하고, payload 는 거래소가 준 그대로.
나중에 로그(Kafka/Redpanda)를 끼워 넣어도 이 봉투 한 줄이 메시지가 된다 (docs/01-design §9).
"""
from __future__ import annotations

import json
import time

SCHEMA_VERSION = 1

SOURCES = {"upbit", "binance"}
STREAMS = {
    ("upbit", "trades"),
    ("upbit", "orderbook"),
    ("binance", "trades"),
    ("binance", "depth"),
}


def now_ms() -> int:
    return int(time.time() * 1000)


def make(source: str, stream: str, payload: dict, recv_ts: int | None = None) -> dict:
    if (source, stream) not in STREAMS:
        raise ValueError(f"unknown stream {source}/{stream}")
    return {
        "source": source,
        "stream": stream,
        "schema_version": SCHEMA_VERSION,
        "recv_ts": recv_ts if recv_ts is not None else now_ms(),
        "payload": payload,
    }


def dumps(env: dict) -> bytes:
    """한 줄 JSON + 개행. ensure_ascii=False 로 원문 그대로, 구분자는 최소."""
    return (json.dumps(env, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")


def loads(line: bytes | str) -> dict:
    return json.loads(line)
