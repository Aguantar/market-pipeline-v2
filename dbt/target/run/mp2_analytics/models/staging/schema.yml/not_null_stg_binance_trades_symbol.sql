
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select symbol
from `mp2`.`stg_binance_trades`
where symbol is null



  
  
    ) dbt_internal_test