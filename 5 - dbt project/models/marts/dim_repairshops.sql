with repairshops as (
  select * 
  from {{ ref('stg_eltool__state') }}
)

select 
  repairshop_id,
  repairshop_name,
  city,
  body,
  mec,
  datetime_created,
  datetime_updated,
  dbt_valid_from::TIMESTAMP as valid_from,
  CASE 
      WHEN dbt_valid_to IS NULL THEN '9999-12-31'::TIMESTAMP
      ELSE dbt_valid_to::TIMESTAMP END AS valid_to
from repaishops
  
