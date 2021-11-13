{{ config(materialized='table') }}

select job_id
  from {{ source('public', 'stg_sitemap') }}
union
select job_id
  from {{ source('public', 'stg_job_description') }}
order by job_id
