
SELECT
    market, toStartOfMinute(trade_ts) AS window_start,
    argMin(price, trade_ts) AS open, max(price) AS high, min(price) AS low, argMax(price, trade_ts) AS close,
    sum(volume) AS volume, sum(amount) AS amount, count() AS trades,
    countIf(taker_side = 'BID') AS buy_trades,
    now64(3) AS computed_at
FROM `mp2`.`stg_upbit_trades`
WHERE trade_ts >= toStartOfDay(now('UTC')) - INTERVAL 1 DAY
GROUP BY market, window_start