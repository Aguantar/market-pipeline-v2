-- raw 를 FINAL 로 읽는 유일한 자리. 시간 규약(v1 docs/34 #3): 대조·품질은 UTC 하루, 리포트는 KST.
SELECT
    market, sequential_id, trade_ts, price, volume, price * volume AS amount,
    ask_bid AS taker_side,
    best_ask_price, best_ask_size, best_bid_price, best_bid_size,
    exchange_ts, recv_ts,
    toUnixTimestamp64Milli(recv_ts) - toUnixTimestamp64Milli(trade_ts) AS lag_ms,
    toStartOfHour(trade_ts) AS hour_utc,
    toDate(trade_ts)        AS day_utc,
    toDate(toTimeZone(trade_ts, 'Asia/Seoul')) AS day_kst
FROM `mp2`.`raw_upbit_trades` FINAL