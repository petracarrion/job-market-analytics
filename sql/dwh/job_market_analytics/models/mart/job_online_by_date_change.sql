SELECT c.online_at
     , c.online_at - 1 AS previous_online_at
     , c.jobs - p.jobs AS difference
FROM {{ ref('count_job_online_by_date') }} c     -- Current
    JOIN {{ ref('count_job_online_by_date') }} p -- Previous
ON c.online_at - 1 = p.online_at