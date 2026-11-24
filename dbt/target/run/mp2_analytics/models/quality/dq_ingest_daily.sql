
  
    
    
    
        
         


        
  

  insert into `mp2`.`dq_ingest_daily`
        ("stream", "day_utc", "rows", "cells_key", "lag_p50_ms", "lag_p95_ms", "first_event", "last_event")
-- 스트림별 일 단위 적재량·지연. 수신-이벤트 지연의 p50/p95 (v1 observation 의 체결 lag 지표를 SQL 로).
SELECT * FROM (
    SELECT 'upbit/trades' AS stream, day_utc, count() AS rows, uniqExact(market) AS cells_key,
           quantile(0.5)(lag_ms) AS lag_p50_ms, quantile(0.95)(lag_ms) AS lag_p95_ms, min(trade_ts) AS first_event, max(trade_ts) AS last_event
    FROM `mp2`.`stg_upbit_trades` GROUP BY day_utc
    UNION ALL
    SELECT 'upbit/orderbook', day_utc, count(), uniqExact(market), quantile(0.5)(lag_ms), quantile(0.95)(lag_ms), min(ts), max(ts)
    FROM `mp2`.`stg_upbit_orderbook` GROUP BY day_utc
    UNION ALL
    SELECT 'binance/trades', day_utc, count(), uniqExact(symbol), quantile(0.5)(lag_ms), quantile(0.95)(lag_ms), min(trade_ts), max(trade_ts)
    FROM `mp2`.`stg_binance_trades` GROUP BY day_utc
    UNION ALL
    SELECT 'binance/depth', day_utc, count(), uniqExact(symbol), 0, 0, min(event_ts), max(event_ts)
    FROM `mp2`.`stg_binance_depth` GROUP BY day_utc
)
  