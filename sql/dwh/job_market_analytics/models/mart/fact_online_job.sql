{{
    config(
        materialized='incremental'
    )
}}


WITH new_fact_online_job AS (
    SELECT online_job.online_at as date_key,
           online_job.online_at,
           online_job.job_id
    FROM {{ source('curated', 'online_job') }}

{% if is_incremental() %}
 LEFT OUTER JOIN fact_online_job
    ON (online_job.online_at = fact_online_job.online_at AND
        online_job.job_id    = fact_online_job.job_id)
    WHERE fact_online_job.job_id IS NULL
{% endif %}
)
SELECT new_fact_online_job.date_key as date_key,
       latest_dim_job.job_key as job_key,
       new_fact_online_job.online_at as online_at,
       latest_dim_job.job_id as job_id,
       latest_dim_job.job_ldts
  FROM new_fact_online_job
 INNER JOIN {{ ref('latest_dim_job') }}
    ON (new_fact_online_job.job_id = latest_dim_job.job_id)