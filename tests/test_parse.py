from decimal import Decimal

from pipeline import envelope, parse, schema


def test_upbit_trade_row_matches_schema():
    p = {"ty": "trade", "cd": "KRW-BTC", "tp": 123456789.0, "tv": 0.00123, "ab": "BID", "ttms": 1795132800123,
         "sid": "1795132800123000", "tms": 1795132800130, "bap": 123456790.0, "bas": 0.5, "bbp": 123456789.0, "bbs": 0.7, "c": "RISE"}
    t, row = parse.parse(envelope.make("upbit", "trades", p, recv_ts=1795132800200))
    assert t.name == "raw_upbit_trades" and len(row) == len(t.columns)
    assert row[0] == "KRW-BTC" and row[2] == 1795132800123000 and row[3] == Decimal("123456789.0") and row[5] == "BID"
    assert row[1].isoformat().startswith("2026-11-20T00:00:00.123")
    assert row[-1].isoformat().startswith("2026-11-20T00:00:00.200")


def test_upbit_trade_without_best_quotes():
    p = {"ty": "trade", "cd": "KRW-XRP", "tp": "1000", "tv": "2", "ab": "ASK", "ttms": 1795132800000, "sid": 5, "tms": 1795132800001}
    t, row = parse.parse(envelope.make("upbit", "trades", p, recv_ts=1795132800002))
    assert row[6] is None and row[9] is None and row[3] == Decimal("1000")


def test_non_data_messages_are_skipped():
    assert parse.parse(envelope.make("upbit", "trades", {"status": "UP"}, recv_ts=1)) is None
    assert parse.parse(envelope.make("upbit", "orderbook", {"ty": "trade"}, recv_ts=1)) is None
    assert parse.parse(envelope.make("binance", "trades", {"e": "aggTrade"}, recv_ts=1)) is None
    assert parse.parse(envelope.make("binance", "depth", {"e": "kline"}, recv_ts=1)) is None


def test_upbit_orderbook_arrays_in_order():
    p = {"ty": "orderbook", "cd": "KRW-BTC", "tms": 1795132800000, "tas": 10.5, "tbs": 20.5, "lv": 0,
         "obu": [{"ap": 101.0, "bp": 99.0, "as": 1.0, "bs": 2.0}, {"ap": 102.0, "bp": 98.0, "as": 3.0, "bs": 4.0}], "st": "REALTIME"}
    t, row = parse.parse(envelope.make("upbit", "orderbook", p, recv_ts=1795132800050))
    assert t.name == "raw_upbit_orderbook"
    assert row[5] == [101.0, 102.0] and row[6] == [1.0, 3.0] and row[7] == [99.0, 98.0] and row[8] == [2.0, 4.0]
    assert row[9] == "REALTIME"


def test_binance_trade_and_depth():
    tr = {"e": "trade", "E": 1795132800100, "s": "BTCUSDT", "t": 42, "p": "90000.10", "q": "0.001", "T": 1795132800099, "m": True, "M": True}
    t, row = parse.parse(envelope.make("binance", "trades", tr, recv_ts=1795132800150))
    assert t.name == "raw_binance_trades" and row[1] == 42 and row[3] == Decimal("90000.10") and row[5] == 1
    d = {"e": "depthUpdate", "E": 1795132800100, "s": "BTCUSDT", "U": 10, "u": 12, "b": [["90000.0", "1.5"]], "a": [["90001.0", "0.2"], ["90002.0", "0"]]}
    t, row = parse.parse(envelope.make("binance", "depth", d, recv_ts=1795132800150))
    assert t.name == "raw_binance_depth" and row[1] == "diff" and row[2] == 10 and row[3] == 12
    assert row[5] == [90000.0] and row[7] == [90001.0, 90002.0] and row[8] == [0.2, 0.0]
    s = {"e": "snapshot", "s": "BTCUSDT", "lastUpdateId": 99, "bids": [["1", "2"]], "asks": [["3", "4"]], "recv_ms": 1}
    t, row = parse.parse(envelope.make("binance", "depth", s, recv_ts=1795132800150))
    assert row[1] == "snapshot" and row[2] == row[3] == 99 and row[5] == [1.0] and row[7] == [3.0]


def test_every_table_has_consistent_keys():
    for t in schema.TABLES.values():
        assert set(t.order_by) <= set(t.columns)
        assert set(t.hash_cols) <= set(t.columns)
        assert t.cell_key in t.columns and t.time_col in t.columns
        assert "recv_ts" in t.columns
