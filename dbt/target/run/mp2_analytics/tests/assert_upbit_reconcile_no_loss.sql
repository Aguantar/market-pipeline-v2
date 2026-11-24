
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- 유실 게이트 (v1 docs/13·17): 대상일(reconcile_date, 기본 어제 UTC)에 거래소 거래량 대비 99% 미만 셀이 없어야 한다. 반환 행 = 실패 셀.

SELECT market, hour_utc, ch_vol, candle_vol, round(ratio_pct, 2) AS ratio_pct, ch_n
FROM `mp2`.`int_upbit_reconcile_hourly`
WHERE toDate(hour_utc) = toDate('2026-11-23') AND candle_vol > 0 AND ratio_pct < 99
  
  
    ) dbt_internal_test