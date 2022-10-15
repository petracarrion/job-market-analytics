{{
    config(
        materialized='table'
    )
}}

SELECT *
  FROM {{ ref('normalized_online_job') }}
 WHERE online_at >= current_date - INTERVAL 12 MONTH
 ORDER BY online_at
