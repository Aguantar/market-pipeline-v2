"""봉투 → raw 행. 파싱은 여기 한 곳에서만 한다 (수집기는 원문만 쓴다).

반환은 schema.RawTable.columns 순서의 튜플. 거를 메시지(구독 응답·heartbeat 등)는 None.
시각은 ms 정수를 그대로 넘기고 ClickHouse 쪽 컬럼이 DateTime64(3) 라 변환은 삽입 시 한다 (parse 는 순수 함수로 두어 테스트가 쉽게).
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from .schema import RawTable, table_for


def _ms(v) -> datetime:
    return datetime.fromtimestamp(int(v) / 1000, tz=timezone.utc)


def _dec(v) -> Decimal:
    return Decimal(str(v))


def _opt_dec(v):
    return None if v is None else _dec(v)


def upbit_trade(p: dict, recv_ts: int):
    # Upbit SIMPLE 포맷: cd 마켓, tp 가격, tv 수량, ab ASK/BID, ttms 체결시각, sid 체결번호, tms 거래소 발신시각, bap/bas/bbp/bbs 최우선 호가
    if p.get("ty") != "trade":
        return None
    return (p["cd"], _ms(p["ttms"]), int(p["sid"]), _dec(p["tp"]), _dec(p["tv"]), p["ab"],
            _opt_dec(p.get("bap")), _opt_dec(p.get("bas")), _opt_dec(p.get("bbp")), _opt_dec(p.get("bbs")),
            _ms(p["tms"]), _ms(recv_ts))


def upbit_orderbook(p: dict, recv_ts: int):
    if p.get("ty") != "orderbook":
        return None
    units = p.get("obu") or []
    return (p["cd"], _ms(p["tms"]), float(p.get("lv") or 0), float(p.get("tas") or 0), float(p.get("tbs") or 0),
            [float(u["ap"]) for u in units], [float(u["as"]) for u in units],
            [float(u["bp"]) for u in units], [float(u["bs"]) for u in units],
            p.get("st") or "REALTIME", _ms(recv_ts))


def binance_trade(p: dict, recv_ts: int):
    if p.get("e") != "trade":
        return None
    return (p["s"], int(p["t"]), _ms(p["T"]), _dec(p["p"]), _dec(p["q"]), 1 if p.get("m") else 0, _ms(p["E"]), _ms(recv_ts))


def binance_depth(p: dict, recv_ts: int):
    e = p.get("e")
    if e == "depthUpdate":
        return (p["s"], "diff", int(p["U"]), int(p["u"]), _ms(p["E"]),
                [float(x[0]) for x in p["b"]], [float(x[1]) for x in p["b"]],
                [float(x[0]) for x in p["a"]], [float(x[1]) for x in p["a"]], _ms(recv_ts))
    if e == "snapshot":
        return (p["s"], "snapshot", int(p["lastUpdateId"]), int(p["lastUpdateId"]), _ms(p.get("E") or recv_ts),
                [float(x[0]) for x in p["bids"]], [float(x[1]) for x in p["bids"]],
                [float(x[0]) for x in p["asks"]], [float(x[1]) for x in p["asks"]], _ms(recv_ts))
    return None


PARSERS = {
    ("upbit", "trades"): upbit_trade,
    ("upbit", "orderbook"): upbit_orderbook,
    ("binance", "trades"): binance_trade,
    ("binance", "depth"): binance_depth,
}


def parse(env: dict):
    """봉투 → (RawTable, row) 또는 None."""
    key = (env["source"], env["stream"])
    row = PARSERS[key](env["payload"], env["recv_ts"])
    if row is None:
        return None
    t: RawTable = table_for(*key)
    assert len(row) == len(t.columns), (t.name, len(row), len(t.columns))
    return t, row
