{{ config(materialized='incremental', incremental_strategy='insert_overwrite', partition_by='toYYYYMMDD(window_start)',
          order_by='(symbol, window_start)', engine='MergeTree()') }}
SELECT
    symbol, toStartOfMinute(trade_ts) AS window_start,
    argMin(price, trade_ts) AS open, max(price) AS high, min(price) AS low, argMax(price, trade_ts) AS close,
    sum(qty) AS volume, sum(quote_qty) AS quote_volume, count() AS trades,
    countIf(taker_side = 'BID') AS buy_trades,
    now64(3) AS computed_at
FROM {{ ref('stg_binance_trades') }}
WHERE {{ minute_window('trade_ts') }}
GROUP BY symbol, window_start
