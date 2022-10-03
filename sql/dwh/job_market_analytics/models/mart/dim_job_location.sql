{{
    config(
        materialized='incremental'
    )
}}


SELECT MD5(CONCAT_WS('||',
            COALESCE(
                job_location.job_id,
                UPPER(TRIM(CAST(
                    job_location.job_id
                AS VARCHAR))),
                NULL,
                '^^'),
            COALESCE(
                job_location.load_timestamp,
                UPPER(TRIM(CAST(
                    job_location.load_timestamp
                AS VARCHAR))),
                NULL,
                '^^')
       )) AS job_key,
       job_location.job_id,
       job_location.load_timestamp as job_ldts,
       job_location.location AS location_name
  FROM {{ source('curated', 'job_location') }}

{% if is_incremental() %}
 LEFT OUTER JOIN dim_job_location
  ON (job_location.job_id   = dim_job_location.job_id AND
      job_location.load_timestamp = dim_job_location.job_ldts AND
      job_location.location = dim_job_location.location_name)
WHERE dim_job_location.job_id IS NULL
{% endif %}