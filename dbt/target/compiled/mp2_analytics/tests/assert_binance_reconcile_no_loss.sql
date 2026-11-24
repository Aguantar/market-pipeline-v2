
SELECT symbol, hour_utc, ex_n, ours_n, round(ratio_pct, 2) AS ratio_pct
FROM `mp2`.`int_binance_reconcile_hourly`
WHERE toDate(hour_utc) = toDate('2026-11-23') AND ex_n > 0 AND ratio_pct < 99