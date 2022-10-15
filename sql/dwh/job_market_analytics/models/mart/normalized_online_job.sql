{{
    config(
        materialized='incremental'
    )
}}

WITH f_created_at AS (
    SELECT DISTINCT online_at
      FROM {{ ref('fact_online_job') }} f
), a_created_at AS (
    SELECT DISTINCT online_at
      FROM {{ this }}
), to_materialize AS (

    SELECT DISTINCT f.online_at
      FROM f_created_at f

    {% if is_incremental() %}
     LEFT OUTER JOIN a_created_at a
       ON (f.online_at = a.online_at)
    WHERE a.online_at IS NULL
    {% endif %}

    ORDER BY 1

)
SELECT f.online_at,
       f.job_id,
       j.company_name,
       l.location_name,
       t.technology_name
  FROM to_materialize tm
  JOIN {{ ref('fact_online_job') }} f
    ON (tm.online_at = f.online_at)
  JOIN {{ ref('dim_job') }} j
    ON (f.job_key = j.job_key)
  JOIN {{ ref('dim_job_location') }} l
    ON (f.job_key = l.job_key)
  JOIN {{ ref('dim_job_technology') }} t
    ON (f.job_key = t.job_key)
