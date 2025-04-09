with source as (
    select * 
    from {{ ref('repaishops_snapshot') }}
),

renamed as (
  select 
    repairshop_id,
    repairshop_name,
    city,
    body,
    mec,
    datetime_created::TIMESTAMP as datetime_created,
    datetime_updated::TIMESTAMP as datetime_updated,
    dbt_valid_from,
    dbt_valid_to
  from source
)

select *
from renamed
    
  
  
    
