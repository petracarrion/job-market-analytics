SELECT online_at,
       COUNT(job_id) AS job_count

FROM {{ source('curated', 'job_online') }}
GROUP BY 1
ORDER BY 1