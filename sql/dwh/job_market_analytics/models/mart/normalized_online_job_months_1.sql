{{
    config(
        materialized='table'
    )
}}

SELECT *
  FROM {{ ref('normalized_online_job') }}
 WHERE online_at >= current_date - INTERVAL 1 MONTH
 ORDER BY online_at
