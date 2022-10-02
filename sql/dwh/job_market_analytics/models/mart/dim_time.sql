{{
    config(
        materialized = 'table',
    )
}}

WITH unique_online_at AS (
    SELECT DISTINCT online_at
      FROM {{ source('curated', 'online_job') }}
     ORDER BY 1
)
SELECT online_at as date_key,
       date_part('year', online_at) as year,
       date_part('month', online_at) as month,
       date_part('day', online_at) as day,
       monthname(online_at) as month_name,
       date_part('yearweek', online_at) as year_week,
       date_part('isodow', online_at) as day_of_week,
       dayname(online_at) as day_of_week_name
  FROM unique_online_at
