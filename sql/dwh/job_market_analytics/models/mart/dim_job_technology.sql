{{
    config(
        materialized='incremental'
    )
}}


SELECT MD5(CONCAT_WS('||',
            COALESCE(
                UPPER(TRIM(CAST(
                    job_technology.job_id
                AS VARCHAR))),
                '^^'),
            COALESCE(
                UPPER(TRIM(CAST(
                    job_technology.load_timestamp
                AS VARCHAR))),
                '^^')
       )) AS job_key,
       job_technology.job_id,
       job_technology.load_timestamp as job_ldts,
       job_technology.technology AS technology_name
  FROM {{ source('curated', 'job_technology') }}

{% if is_incremental() %}
 LEFT OUTER JOIN dim_job_technology
  ON (job_technology.job_id         = dim_job_technology.job_id AND
      job_technology.load_timestamp = dim_job_technology.job_ldts AND
      job_technology.technology     = dim_job_technology.technology_name)
WHERE dim_job_technology.job_id IS NULL
{% endif %}
