{{
    config(
        materialized='incremental'
    )
}}


SELECT online_at     AS date_id,
       COUNT(job_id) AS total
FROM {{ source('curated', 'job_online') }}

{% if is_incremental() %}
WHERE online_at > (
    SELECT max(date_id)
    FROM {{ this }}
)
{% endif %}

GROUP BY 1
ORDER BY 1