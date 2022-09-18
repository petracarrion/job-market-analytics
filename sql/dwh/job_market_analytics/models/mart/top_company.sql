WITH unique_job_company AS (
    SELECT DISTINCT company_name, job_id
    FROM {{ source('curated', 'job_description') }}
)

SELECT company_name AS company, count(job_id)
FROM unique_job_company
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5