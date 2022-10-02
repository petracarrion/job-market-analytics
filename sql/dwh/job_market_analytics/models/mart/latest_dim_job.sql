SELECT job_key,
       job_id,
       job_ldts,
       title,
       company_name
  FROM (
    SELECT job_key,
           job_id,
           job_ldts,
           title,
           company_name,
           ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY job_ldts DESC) rn
      FROM {{ ref('dim_job') }}
    )
WHERE rn = 1
