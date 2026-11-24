{{ config(materialized='incremental', incremental_strategy='insert_overwrite', partition_by='toYYYYMMDD(window_start)',
          order_by='(market, window_start)', engine='MergeTree()') }}
-- v1 Flink OrderbookAggregator 와 같은 식 (v1 docs/58 §5-2 에서 2,946행 19컬럼 일치 검증). 파티션(일) 교체라 재실행이 멱등.
SELECT
    market,
    toStartOfMinute(ts) AS window_start,
    window_start + INTERVAL 1 MINUTE AS window_end,
    toUInt32(count()) AS snapshots,
    argMin(mid, ts) AS mid_open, argMax(mid, ts) AS mid_close, min(mid) AS mid_min, max(mid) AS mid_max,
    avg(best_ask - best_bid) AS spread_avg,
    avg((best_ask - best_bid) / mid * 10000) AS spread_bp_avg,
    max((best_ask - best_bid) / mid * 10000) AS spread_bp_max,
    avg(if(bid_sizes[1] + ask_sizes[1] > 0, (bid_sizes[1] - ask_sizes[1]) / (bid_sizes[1] + ask_sizes[1]), 0)) AS imb1_avg,
    avg(if(arraySum(arraySlice(bid_sizes, 1, 5)) + arraySum(arraySlice(ask_sizes, 1, 5)) > 0,
           (arraySum(arraySlice(bid_sizes, 1, 5)) - arraySum(arraySlice(ask_sizes, 1, 5))) / (arraySum(arraySlice(bid_sizes, 1, 5)) + arraySum(arraySlice(ask_sizes, 1, 5))), 0)) AS imb5_avg,
    avg(if(arraySum(arraySlice(bid_sizes, 1, 15)) + arraySum(arraySlice(ask_sizes, 1, 15)) > 0,
           (arraySum(arraySlice(bid_sizes, 1, 15)) - arraySum(arraySlice(ask_sizes, 1, 15))) / (arraySum(arraySlice(bid_sizes, 1, 15)) + arraySum(arraySlice(ask_sizes, 1, 15))), 0)) AS imb15_avg,
    avg(arraySum(arraySlice(ask_sizes, 1, 15))) AS ask_depth15_avg,
    avg(arraySum(arraySlice(bid_sizes, 1, 15))) AS bid_depth15_avg,
    avg(total_ask_size) AS total_ask_avg, avg(total_bid_size) AS total_bid_avg,
    avg(lag_ms) AS recv_lag_ms_avg,
    now64(3) AS computed_at
FROM {{ ref('stg_upbit_orderbook') }}
WHERE {{ minute_window('ts') }}
GROUP BY market, window_start
