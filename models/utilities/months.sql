with days as (
    SELECT *
    FROM {{ref('dates')}}
),

months as (
    SELECT DISTINCT
         DATE_TRUNC('month', date_day) as date_month
    FROM days
)

SELECT *
FROM months