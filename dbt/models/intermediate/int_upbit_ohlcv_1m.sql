{{ config(materialized='incremental', incremental_strategy='insert_overwrite', partition_by='toYYYYMMDD(window_start)',
          order_by='(market, window_start)', engine='MergeTree()') }}
SELECT
    market, toStartOfMinute(trade_ts) AS window_start,
    argMin(price, trade_ts) AS open, max(price) AS high, min(price) AS low, argMax(price, trade_ts) AS close,
    sum(volume) AS volume, sum(amount) AS amount, count() AS trades,
    countIf(taker_side = 'BID') AS buy_trades,
    now64(3) AS computed_at
FROM {{ ref('stg_upbit_trades') }}
WHERE {{ minute_window('trade_ts') }}
GROUP BY market, window_start
