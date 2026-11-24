"""파서의 단일 출처 (docs/01-design §10 #3). 봉투(source, stream, schema_version, recv_ts DateTime64(3), payload String) → raw 행.

이 모듈이 만드는 SELECT 식을 세 곳이 그대로 쓴다:
  1) Materialized View (라이브: envelopes Null 테이블 → raw_*)      → clickhouse/ddl/005_envelopes_mv.sql (generate_ddl)
  2) rebuild (파일 → envelopes → 같은 MV)                            → 라이브와 재처리가 같은 파서를 지난다
  3) 컬럼형 Parquet 내보내기 (파일 → 파싱 → Parquet)                   → compact.py
파이썬 parse.py 는 수집기 단위 테스트용으로 남고, tests/test_parity 가 "같은 봉투 → 같은 행" 을 보장한다.

표기 규칙: 금액·수량은 JSON 원문 텍스트를 Decimal128(8) 로 (toFloat64 를 거치지 않는다 - 16자리 정밀도 손실 방지). 호가 배열은 Float64 (v1 과 같음).
주의: 이 SELECT 를 실행하는 세션은 `precise_float_parsing=1` 이어야 파이썬 float 와 비트까지 같다 (toFloat64 기본은 비정확 반올림, 11-24 동치 테스트).
"""
from __future__ import annotations

from .schema import TABLES, RawTable

ENVELOPE_COLUMNS = "source String, stream String, schema_version UInt8, recv_ts DateTime64(3, 'UTC'), payload String"


def _dec(path: str) -> str:
    return f"toDecimal128(JSONExtractRaw(payload, '{path}'), 8)"


def _opt_dec(path: str) -> str:
    return f"if(JSONHas(payload, '{path}'), {_dec(path)}, NULL)"


def _ms(expr: str) -> str:
    return f"fromUnixTimestamp64Milli({expr}, 'UTC')"


# 각 항목: (컬럼명, SQL 식). 순서는 schema.RawTable.columns 와 같아야 한다 (generate_ddl 이 검증).
SELECTS: dict[tuple[str, str], tuple[str, list[tuple[str, str]]]] = {
    ("upbit", "trades"): (
        "source = 'upbit' AND stream = 'trades' AND JSONExtractString(payload, 'ty') = 'trade'",
        [("market", "JSONExtractString(payload, 'cd')"),
         ("trade_ts", _ms("JSONExtractInt(payload, 'ttms')")),
         ("sequential_id", "JSONExtractUInt(payload, 'sid')"),
         ("price", _dec("tp")), ("volume", _dec("tv")),
         ("ask_bid", "JSONExtractString(payload, 'ab')"),
         ("best_ask_price", _opt_dec("bap")), ("best_ask_size", _opt_dec("bas")),
         ("best_bid_price", _opt_dec("bbp")), ("best_bid_size", _opt_dec("bbs")),
         ("exchange_ts", _ms("JSONExtractInt(payload, 'tms')")),
         ("recv_ts", "{recv}")]),
    ("upbit", "orderbook"): (
        "source = 'upbit' AND stream = 'orderbook' AND JSONExtractString(payload, 'ty') = 'orderbook'",
        [("market", "JSONExtractString(payload, 'cd')"),
         ("ts", _ms("JSONExtractInt(payload, 'tms')")),
         ("level", "JSONExtractFloat(payload, 'lv')"),
         ("total_ask_size", "JSONExtractFloat(payload, 'tas')"), ("total_bid_size", "JSONExtractFloat(payload, 'tbs')"),
         ("ask_prices", "arrayMap(u -> JSONExtractFloat(u, 'ap'), JSONExtractArrayRaw(payload, 'obu'))"),
         ("ask_sizes", "arrayMap(u -> JSONExtractFloat(u, 'as'), JSONExtractArrayRaw(payload, 'obu'))"),
         ("bid_prices", "arrayMap(u -> JSONExtractFloat(u, 'bp'), JSONExtractArrayRaw(payload, 'obu'))"),
         ("bid_sizes", "arrayMap(u -> JSONExtractFloat(u, 'bs'), JSONExtractArrayRaw(payload, 'obu'))"),
         ("stream_type", "if(JSONHas(payload, 'st') AND JSONExtractString(payload, 'st') != '', JSONExtractString(payload, 'st'), 'REALTIME')"),
         ("recv_ts", "{recv}")]),
    ("binance", "trades"): (
        "source = 'binance' AND stream = 'trades' AND JSONExtractString(payload, 'e') = 'trade'",
        [("symbol", "JSONExtractString(payload, 's')"),
         ("trade_id", "JSONExtractUInt(payload, 't')"),
         ("trade_ts", _ms("JSONExtractInt(payload, 'T')")),
         ("price", "toDecimal128(JSONExtractString(payload, 'p'), 8)"), ("qty", "toDecimal128(JSONExtractString(payload, 'q'), 8)"),
         ("is_buyer_maker", "toUInt8(JSONExtractBool(payload, 'm'))"),
         ("event_ts", _ms("JSONExtractInt(payload, 'E')")),
         ("recv_ts", "{recv}")]),
    ("binance", "depth"): (
        "source = 'binance' AND stream = 'depth' AND JSONExtractString(payload, 'e') IN ('depthUpdate', 'snapshot')",
        [("symbol", "JSONExtractString(payload, 's')"),
         ("kind", "if(JSONExtractString(payload, 'e') = 'snapshot', 'snapshot', 'diff')"),
         ("first_update_id", "if(JSONExtractString(payload, 'e') = 'snapshot', JSONExtractUInt(payload, 'lastUpdateId'), JSONExtractUInt(payload, 'U'))"),
         ("last_update_id", "if(JSONExtractString(payload, 'e') = 'snapshot', JSONExtractUInt(payload, 'lastUpdateId'), JSONExtractUInt(payload, 'u'))"),
         ("event_ts", f"if(JSONHas(payload, 'E'), {_ms('JSONExtractInt(payload, ' + chr(39) + 'E' + chr(39) + ')')}, {{recv}})"),
         ("bid_prices", "arrayMap(x -> toFloat64(JSONExtractString(x, 1)), JSONExtractArrayRaw(payload, if(JSONExtractString(payload, 'e') = 'snapshot', 'bids', 'b')))"),
         ("bid_sizes", "arrayMap(x -> toFloat64(JSONExtractString(x, 2)), JSONExtractArrayRaw(payload, if(JSONExtractString(payload, 'e') = 'snapshot', 'bids', 'b')))"),
         ("ask_prices", "arrayMap(x -> toFloat64(JSONExtractString(x, 1)), JSONExtractArrayRaw(payload, if(JSONExtractString(payload, 'e') = 'snapshot', 'asks', 'a')))"),
         ("ask_sizes", "arrayMap(x -> toFloat64(JSONExtractString(x, 2)), JSONExtractArrayRaw(payload, if(JSONExtractString(payload, 'e') = 'snapshot', 'asks', 'a')))"),
         ("recv_ts", "{recv}")]),
}


RECV_FROM_TABLE = "recv_ts"                                    # envelopes 테이블: DateTime64(3)
RECV_FROM_FILE = "fromUnixTimestamp64Milli(recv_ts, 'UTC')"    # 봉투 JSONL 파일: Int64 ms


def select_sql(key: tuple[str, str], from_clause: str, recv: str = RECV_FROM_TABLE) -> str:
    """봉투 소스(from_clause: 테이블명 또는 file(...) 식)에서 raw 행을 뽑는 SELECT. recv 는 봉투 수신시각 식."""
    where, cols = SELECTS[key]
    t: RawTable = TABLES[key]
    assert tuple(c for c, _ in cols) == t.columns, (key, [c for c, _ in cols], t.columns)
    body = ",\n    ".join(f"{expr.replace('{recv}', recv)} AS {name}" for name, expr in cols)
    return f"SELECT\n    {body}\nFROM {from_clause}\nWHERE {where}"


def generate_ddl(db: str = "mp2") -> str:
    out = [f"-- GENERATED by pipeline/sql_parse.py - 손으로 고치지 말 것. 파서의 단일 출처는 sql_parse.SELECTS (docs/01-design §10 #3).",
           f"CREATE TABLE IF NOT EXISTS {db}.envelopes ({ENVELOPE_COLUMNS}) ENGINE = Null;  -- 저장하지 않는다. 원본은 파일, 파싱 결과는 MV 가 raw_* 로"]
    for key in SELECTS:
        t = TABLES[key]
        sel = select_sql(key, f"{db}.envelopes").replace("\n", "\n    ")
        out.append(f"CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.mv_{t.name} TO {db}.{t.name} AS\n    {sel};")
    return "\n\n".join(out) + "\n"


if __name__ == "__main__":
    import sys
    sys.stdout.write(generate_ddl(sys.argv[1] if len(sys.argv) > 1 else "mp2"))
