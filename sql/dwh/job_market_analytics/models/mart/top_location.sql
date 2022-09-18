WITH unique_job_location AS
         (SELECT DISTINCT location, job_id
          FROM {{ source('curated', 'job_location') }})

SELECT location, count(job_id)
FROM unique_job_location
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5