with jobs as (
  select * 
  from {{ ref('stg_eltool__jobs') }}
)

select * from jobs
