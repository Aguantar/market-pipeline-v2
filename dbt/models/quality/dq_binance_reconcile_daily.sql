{{ config(order_by='day_utc') }}
SELECT toDate(hour_utc) AS day_utc,
       round(100 * sum(ours_n) / sum(ex_n), 3) AS weighted_pct, count() AS cells,
       countIf(ratio_pct < 99) AS cells_below_99, countIf(ratio_pct > 101) AS cells_above_101, countIf(ours_n = 0) AS cells_no_rows,
       round(min(ratio_pct), 2) AS min_cell_pct, argMin(concat(symbol, ' ', toString(hour_utc)), ratio_pct) AS worst_cell,
       max(reconciled_at) AS reconciled_at
FROM {{ ref('int_binance_reconcile_hourly') }} WHERE ex_n > 0 GROUP BY day_utc
