with forecasts as (
    SELECT *
    FROM {{source('datacaudata', 'forecasts')}}
)

SELECT *
FROM forecasts
