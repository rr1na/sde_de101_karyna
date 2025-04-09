with source as (
  select * 
  from {{ source('raw_layer', 'orders') }}
),
renamed as (
  select
    job_id,
    plate,
    type,
    repairshop_id,
    order_carrier_date::TIMESTAMP AS order_carrier_date
  from source
)

select *
from renamed
    
