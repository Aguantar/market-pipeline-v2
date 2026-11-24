
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sequential_id
from `mp2`.`stg_upbit_trades`
where sequential_id is null



  
  
    ) dbt_internal_test