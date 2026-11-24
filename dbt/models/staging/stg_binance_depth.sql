SELECT symbol, kind, first_update_id, last_update_id, event_ts, bid_prices, bid_sizes, ask_prices, ask_sizes, recv_ts,
       toStartOfHour(event_ts) AS hour_utc, toDate(event_ts) AS day_utc
FROM {{ source('raw', 'raw_binance_depth') }} FINAL
