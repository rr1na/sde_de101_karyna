{% snapshot jobs_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='repairshop_id',

      stratedy='timestamp',
      update_at='datetime_update',
    )
}}

select * from {{ source('raw_layer', 'repairshops') }}

(% endsnapshot %)
      
