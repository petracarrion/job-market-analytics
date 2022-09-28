WITH unique_job_company AS (
    SELECT DISTINCT company_name, job_id
    FROM {{ source('curated', 'job') }}
)

SELECT company_name AS company, COUNT(job_id) AS job_count
FROM unique_job_company
GROUP BY 1
ORDER BY 2 DESC
LIMIT 50