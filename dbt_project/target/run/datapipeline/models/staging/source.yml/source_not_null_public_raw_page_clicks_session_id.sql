
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select session_id
from "ecommerce"."public"."raw_page_clicks"
where session_id is null



  
  
      
    ) dbt_internal_test