with repairshops as (
  select * 
  from {{ ref('dim_repairshops') }},
jobs as (
  select * 
  from {{ ref('fct__jobs') }}
)

select 
  r.repairshop_name,
  r.city as repairhsop_city,
  COUNT(*) as number_of_jobs
from repaishops r LEFT JOIN jobs j ON r.repairshop_id=j.repairshop_id
