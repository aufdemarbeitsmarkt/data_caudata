with numbers as (
    SELECT *
    FROM {{ref('numbers')}}
),

days as (
    SELECT '2008-01-01'::DATE + numbers as date_day
    FROM numbers
)

SELECT date_day
FROM days