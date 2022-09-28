SELECT online_at,
       COUNT(job_id) AS job_count

FROM {{ source('curated', 'online_job') }}
GROUP BY 1
ORDER BY 1