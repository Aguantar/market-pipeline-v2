SELECT
    symbol, trade_id, trade_ts, price, qty, price * qty AS quote_qty,
    if(is_buyer_maker = 1, 'ASK', 'BID') AS taker_side,
    event_ts, recv_ts,
    toUnixTimestamp64Milli(recv_ts) - toUnixTimestamp64Milli(trade_ts) AS lag_ms,
    toStartOfHour(trade_ts) AS hour_utc, toDate(trade_ts) AS day_utc
FROM {{ source('raw', 'raw_binance_trades') }} FINAL
