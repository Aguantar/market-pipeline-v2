"""mp2 ClickHouse 가 떠 있을 때만 도는 통합 테스트. 없으면 skip."""
import os
from pathlib import Path

import pytest

from pipeline import chsink, envelope, parse, schema

ENV = Path(__file__).resolve().parents[1] / ".env"


def _client():
    if not ENV.exists():
        pytest.skip("no .env")
    pw = next((l.split("=", 1)[1].strip() for l in ENV.read_text().splitlines() if l.startswith("CLICKHOUSE_PASSWORD=")), "")
    try:
        c = chsink.make_client("127.0.0.1", 8123, "default", pw, "mp2")
        c.command("SELECT 1")
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"clickhouse not reachable: {e}")
    return c


SAMPLES = {
    ("upbit", "trades"): {"ty": "trade", "cd": "KRW-TEST", "tp": 100.5, "tv": 2, "ab": "BID", "ttms": 1795132800123, "sid": 777, "tms": 1795132800130},
    ("upbit", "orderbook"): {"ty": "orderbook", "cd": "KRW-TEST", "tms": 1795132800000, "tas": 1, "tbs": 2, "obu": [{"ap": 1, "bp": 0.9, "as": 1, "bs": 1}], "st": "REALTIME"},
    ("binance", "trades"): {"e": "trade", "E": 1795132800100, "s": "TESTUSDT", "t": 1, "p": "1.5", "q": "2", "T": 1795132800099, "m": False},
    ("binance", "depth"): {"e": "depthUpdate", "E": 1795132800100, "s": "TESTUSDT", "U": 1, "u": 2, "b": [["1", "1"]], "a": [["2", "1"]]},
}


@pytest.fixture
def client():
    c = _client()
    for t in schema.TABLES.values():
        c.command(f"ALTER TABLE {t.name} DELETE WHERE {t.cell_key} IN ('KRW-TEST','TESTUSDT') SETTINGS mutations_sync=2")
    yield c
    for t in schema.TABLES.values():
        c.command(f"ALTER TABLE {t.name} DELETE WHERE {t.cell_key} IN ('KRW-TEST','TESTUSDT') SETTINGS mutations_sync=2")


def test_insert_each_table_and_idempotent(client):
    for key, payload in SAMPLES.items():
        t, row = parse.parse(envelope.make(*key, payload, recv_ts=1795132800200))
        b = chsink.CHBatcher(client, t, batch_rows=10, batch_sec=0.01)
        b.add(row); b.add(row)          # 같은 행 두 번 (재전송 흉내)
        b.flush()
        assert b.inserted == 2 and b.pending_rows == 0 and b.dropped == 0, b.stats()
        n_final = client.command(f"SELECT count() FROM {t.name} FINAL WHERE {t.cell_key} IN ('KRW-TEST','TESTUSDT')")
        assert n_final == 1, (t.name, n_final)   # ReplacingMergeTree + 자연키 → FINAL 1행


def test_failed_insert_is_retried_not_lost(client):
    t = schema.table_for("upbit", "trades")
    _, row = parse.parse(envelope.make("upbit", "trades", SAMPLES[("upbit", "trades")], recv_ts=1795132800200))

    class Flaky:
        def __init__(self, real): self.real, self.fail = real, 2
        def insert(self, *a, **k):
            if self.fail:
                self.fail -= 1
                raise RuntimeError("simulated outage")
            return self.real.insert(*a, **k)

    b = chsink.CHBatcher(Flaky(client), t, batch_rows=1, batch_sec=0.0)
    b.add(row); b.flush()
    assert b.failed_batches == 1 and b.pending_rows == 1 and b.inserted == 0
    b.next_retry = 0; b.flush()
    assert b.failed_batches == 2 and b.pending_rows == 1
    b.next_retry = 0; b.flush()
    assert b.inserted == 1 and b.pending_rows == 0 and b.backoff == 1.0


def test_queue_cap_drops_oldest_and_counts(client):
    t = schema.table_for("binance", "trades")
    _, row = parse.parse(envelope.make("binance", "trades", SAMPLES[("binance", "trades")], recv_ts=1))

    class Down:
        def insert(self, *a, **k): raise RuntimeError("down")

    b = chsink.CHBatcher(Down(), t, batch_rows=100, batch_sec=0.0, max_queue=250)
    for _ in range(5):
        for _ in range(100):
            b.add(row)
        b.next_retry = 0; b.flush()
    assert b.dropped == 300 and b.pending_rows == 200
