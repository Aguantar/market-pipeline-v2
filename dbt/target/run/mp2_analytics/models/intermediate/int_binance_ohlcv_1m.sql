
            
  
    
    
    
        
         


        
  

  insert into `mp2`.`int_binance_ohlcv_1m__dbt_new_data_d8b7636c_19a2_443d_bef7_802e211428a2`
        ("symbol", "window_start", "open", "high", "low", "close", "volume", "quote_volume", "trades", "buy_trades", "computed_at")
SELECT
    symbol, toStartOfMinute(trade_ts) AS window_start,
    argMin(price, trade_ts) AS open, max(price) AS high, min(price) AS low, argMax(price, trade_ts) AS close,
    sum(qty) AS volume, sum(quote_qty) AS quote_volume, count() AS trades,
    countIf(taker_side = 'BID') AS buy_trades,
    now64(3) AS computed_at
FROM `mp2`.`stg_binance_trades`
WHERE trade_ts >= toStartOfDay(now('UTC')) - INTERVAL 1 DAY
GROUP BY symbol, window_start
  
        