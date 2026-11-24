"""raw 테이블 정의와 멱등 키 (docs/01-design §4). 파서(parse.py)·DDL(clickhouse/ddl)·대조(rebuild/compare.py) 가 이 한 곳을 본다.

cell_key: 대조 셀의 분할 키(마켓/심볼). time_col: 셀의 시간 축(이벤트 시각). hash_cols: 전행 해시에 넣는 값 컬럼(멱등 키 + 값).
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class RawTable:
    name: str
    source: str
    stream: str
    columns: tuple[str, ...]          # INSERT 컬럼 순서 (파서 출력 순서와 같다)
    order_by: tuple[str, ...]         # ReplacingMergeTree 정렬키 = 멱등 키
    cell_key: str
    time_col: str
    hash_cols: tuple[str, ...]


TABLES: dict[tuple[str, str], RawTable] = {
    ("upbit", "trades"): RawTable(
        name="raw_upbit_trades", source="upbit", stream="trades",
        columns=("market", "trade_ts", "sequential_id", "price", "volume", "ask_bid",
                 "best_ask_price", "best_ask_size", "best_bid_price", "best_bid_size", "exchange_ts", "recv_ts"),
        order_by=("market", "sequential_id", "trade_ts"),
        cell_key="market", time_col="trade_ts",
        hash_cols=("market", "sequential_id", "trade_ts", "price", "volume", "ask_bid"),
    ),
    ("upbit", "orderbook"): RawTable(
        name="raw_upbit_orderbook", source="upbit", stream="orderbook",
        columns=("market", "ts", "level", "total_ask_size", "total_bid_size",
                 "ask_prices", "ask_sizes", "bid_prices", "bid_sizes", "stream_type", "recv_ts"),
        order_by=("market", "ts"),
        cell_key="market", time_col="ts",
        hash_cols=("market", "ts", "total_ask_size", "total_bid_size", "ask_prices", "ask_sizes", "bid_prices", "bid_sizes"),
    ),
    ("binance", "trades"): RawTable(
        name="raw_binance_trades", source="binance", stream="trades",
        columns=("symbol", "trade_id", "trade_ts", "price", "qty", "is_buyer_maker", "event_ts", "recv_ts"),
        order_by=("symbol", "trade_id"),
        cell_key="symbol", time_col="trade_ts",
        hash_cols=("symbol", "trade_id", "trade_ts", "price", "qty", "is_buyer_maker"),
    ),
    ("binance", "depth"): RawTable(
        name="raw_binance_depth", source="binance", stream="depth",
        columns=("symbol", "kind", "first_update_id", "last_update_id", "event_ts",
                 "bid_prices", "bid_sizes", "ask_prices", "ask_sizes", "recv_ts"),
        order_by=("symbol", "last_update_id", "kind"),
        cell_key="symbol", time_col="event_ts",
        hash_cols=("symbol", "kind", "first_update_id", "last_update_id", "bid_prices", "bid_sizes", "ask_prices", "ask_sizes"),
    ),
}


def table_for(source: str, stream: str) -> RawTable:
    return TABLES[(source, stream)]


# 봉투 Null 테이블 (파싱은 MV 가 한다, sql_parse.py). CHBatcher 가 insert 대상으로 쓰는 최소 정의.
ENVELOPES = RawTable(
    name="envelopes", source="*", stream="*",
    columns=("source", "stream", "schema_version", "recv_ts", "payload"),
    order_by=(), cell_key="source", time_col="recv_ts", hash_cols=("payload",),
)

# 통계용 이벤트 시각 추출 (파서가 아니다 - 지연 p50/p95 계산에만 쓴다)
EVENT_TS_KEY: dict[tuple[str, str], str] = {
    ("upbit", "trades"): "ttms", ("upbit", "orderbook"): "tms",
    ("binance", "trades"): "T", ("binance", "depth"): "E",
}
