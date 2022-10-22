{{
    config(
        materialized='incremental'
    )
}}

WITH f_created_at AS (
    SELECT DISTINCT online_at
      FROM {{ ref('fact_online_job') }} f
), max_normalized AS (
    SELECT MAX(online_at)
      FROM {{ this }}
), n_created_at AS (
    SELECT DISTINCT online_at
      FROM {{ this }}
), n_created_at_without_max AS (
    SELECT *
      FROM n_created_at
     WHERE online_at != (SELECT * FROM max_normalized)
     ORDER BY 1
), to_materialize AS (

    SELECT DISTINCT f.online_at
      FROM f_created_at f

{% if is_incremental() %}
     LEFT OUTER JOIN n_created_at_without_max n
       ON (f.online_at = n.online_at)
    WHERE n.online_at IS NULL
{% endif %}

    ORDER BY 1

), normalized AS (
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
     ORDER BY 1
), normalized_with_previous AS (
    SELECT job_id,
           location_name,
           company_name,
           technology_name,
           online_at,
           online_at - INTERVAL 1 DAY AS previous_day
      FROM normalized
), min_online_at AS (
    SELECT MIN(online_at)
      FROM normalized_with_previous
), max_online_at AS (
    SELECT MAX(online_at)
      FROM normalized_with_previous
), joined_normalized_with_previous AS (
    SELECT DISTINCT
           c.job_id,
           c.online_at,
           c.location_name,
           c.company_name,
           c.technology_name,
           p.location_name AS previous_location_name,
           p.company_name AS previous_company_name,
           p.technology_name AS previous_technology_name,
           p.online_at AS previous_online_at,
           p.job_id AS previous_job_id
      FROM normalized_with_previous c
      FULL OUTER JOIN normalized_with_previous p
        ON (c.job_id = p.job_id AND
            c.location_name = p.location_name AND
            c.company_name = p.company_name AND
            c.technology_name = p.technology_name AND
            c.previous_day = p.online_at)
)
SELECT COALESCE(job_id, previous_job_id) AS job_id,
       COALESCE(location_name, previous_location_name) AS location_name,
       COALESCE(company_name, previous_company_name) AS company_name,
       COALESCE(technology_name, previous_technology_name) AS technology_name,
       COALESCE(online_at, previous_online_at) AS online_at,
       previous_job_id IS NULL AS added,
       job_id IS NULL AS deleted
  FROM joined_normalized_with_previous
 WHERE NOT COALESCE(online_at, previous_online_at) IN (SELECT * FROM max_online_at)
   AND NOT COALESCE(online_at, previous_online_at) IN (SELECT * FROM min_online_at)
