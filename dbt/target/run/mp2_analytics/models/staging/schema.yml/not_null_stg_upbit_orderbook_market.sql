
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select market
from `mp2`.`stg_upbit_orderbook`
where market is null



  
  
    ) dbt_internal_test