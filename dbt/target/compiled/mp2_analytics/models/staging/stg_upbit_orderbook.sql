SELECT
    market, ts, level, total_ask_size, total_bid_size,
    ask_prices, ask_sizes, bid_prices, bid_sizes, stream_type, recv_ts,
    ask_prices[1] AS best_ask, bid_prices[1] AS best_bid,
    (ask_prices[1] + bid_prices[1]) / 2 AS mid,
    toUnixTimestamp64Milli(recv_ts) - toUnixTimestamp64Milli(ts) AS lag_ms,
    toStartOfHour(ts) AS hour_utc, toDate(ts) AS day_utc
FROM `mp2`.`raw_upbit_orderbook` FINAL
WHERE length(ask_prices) > 0 AND length(bid_prices) > 0 AND ask_prices[1] > 0 AND bid_prices[1] > 0