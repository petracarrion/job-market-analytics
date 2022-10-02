{{
  config(
    materialized = 'table',
    )
}}

SELECT range as date_key,
       date_part('year', range) as year,
       date_part('month', range) as month,
       date_part('day', range) as day,
       monthname(range) as monthname,
       date_part('yearweek', range) as yearweek,
       date_part('isodow', range) as isodow,
       dayname(range) as dayname
  FROM range(TIMESTAMP '2021-01-01', TIMESTAMP '2023-01-01', INTERVAL 1 DAY)
