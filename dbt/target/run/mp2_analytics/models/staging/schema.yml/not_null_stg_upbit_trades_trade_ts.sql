
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trade_ts
from `mp2`.`stg_upbit_trades`
where trade_ts is null



  
  
    ) dbt_internal_test