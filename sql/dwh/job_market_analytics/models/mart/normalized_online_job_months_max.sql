{{
    config(
        materialized='view'
    )
}}

SELECT *
  FROM {{ ref('normalized_online_job') }}
 ORDER BY online_at
