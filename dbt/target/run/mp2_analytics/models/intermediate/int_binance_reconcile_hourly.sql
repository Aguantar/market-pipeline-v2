
  
    
    
    
        
         


        
  

  insert into `mp2`.`int_binance_reconcile_hourly`
        ("symbol", "hour_utc", "ex_n", "ours_n", "ratio_pct", "reconciled_at")
-- Binance 는 1h 캔들의 체결 수(n) 가 진실값 (v1 docs/31 §3-2).
WITH ex AS (SELECT symbol, hour_utc, trade_count AS ex_n FROM `mp2`.`binance_hourly_candles` FINAL),
bounds AS (SELECT min(hour_utc) AS h_min, max(hour_utc) + INTERVAL 1 HOUR AS h_max FROM ex),
ours AS (
    SELECT symbol, hour_utc, count() AS ours_n
    FROM `mp2`.`stg_binance_trades`
    WHERE trade_ts >= (SELECT h_min FROM bounds) AND trade_ts < (SELECT h_max FROM bounds)
    GROUP BY symbol, hour_utc
)
SELECT e.symbol, e.hour_utc, e.ex_n, ifNull(o.ours_n, 0) AS ours_n,
       if(e.ex_n > 0, 100.0 * ifNull(o.ours_n, 0) / e.ex_n, NULL) AS ratio_pct, now() AS reconciled_at
FROM ex e LEFT JOIN ours o ON o.symbol = e.symbol AND o.hour_utc = e.hour_utc
  