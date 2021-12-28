{{ config(materialized='table') }}

select *
from (select job_id
      from {{ source('public', 'stg_sitemap') }}
      union
      select job_id
      from {{ source('public', 'stg_job_description') }}
     ) as t
where job_id is not null
order by job_id
