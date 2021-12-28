{{ config(materialized='table') }}

select sitemap_ingestion_hashkey,
       timestamp as load_date,
       job_id,
       url,
       True      as is_online
from {{ source('public', 'stg_sitemap') }}
where job_id is not null
order by timestamp, job_id
