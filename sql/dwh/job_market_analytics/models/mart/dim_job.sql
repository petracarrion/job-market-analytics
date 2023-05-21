{{
    config(
        materialized='incremental'
    )
}}


SELECT MD5(CONCAT_WS('||',
            COALESCE(
                UPPER(TRIM(CAST(
                    job.job_id
                AS VARCHAR))),
                '^^'),
            COALESCE(
                UPPER(TRIM(CAST(
                    job.load_timestamp
                AS VARCHAR))),
                '^^')
       )) AS job_key,
       job.job_id,
       job.load_timestamp as job_ldts,
       job.title,
       job.company_name
  FROM {{ source('curated', 'job') }}

{% if is_incremental() %}
 LEFT OUTER JOIN dim_job
  ON (job.job_id   = dim_job.job_id AND
      job.load_timestamp = dim_job.job_ldts)
WHERE dim_job.job_id IS NULL
{% endif %}
