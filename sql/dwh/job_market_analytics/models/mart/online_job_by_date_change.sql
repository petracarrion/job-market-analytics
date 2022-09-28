SELECT c.online_at
     , c.online_at - 1           AS previous_online_at
     , c.job_count - p.job_count AS difference
FROM {{ ref('count_online_job_by_date') }} c     -- Current
    JOIN {{ ref('count_online_job_by_date') }} p -- Previous
ON c.online_at - 1 = p.online_at