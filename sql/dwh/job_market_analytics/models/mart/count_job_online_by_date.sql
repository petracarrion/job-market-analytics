SELECT online_at,
       count(job_id) as jobs

FROM {{ source('curated', 'job_online') }}
GROUP BY 1
ORDER BY 1