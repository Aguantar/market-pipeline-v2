{{ config(order_by='day_utc') }}
SELECT toDate(hour_utc) AS day_utc,
       round(100 * sum(ch_vol) / sum(candle_vol), 3) AS weighted_pct, count() AS cells,
       countIf(ratio_pct < 99) AS cells_below_99, countIf(ch_n = 0) AS cells_no_rows, uniqExactIf(market, ch_n = 0) AS markets_no_rows,
       round(min(ratio_pct), 2) AS min_cell_pct, argMin(concat(market, ' ', toString(hour_utc)), ratio_pct) AS worst_cell,
       max(reconciled_at) AS reconciled_at
FROM {{ ref('int_upbit_reconcile_hourly') }} WHERE candle_vol > 0 GROUP BY day_utc
