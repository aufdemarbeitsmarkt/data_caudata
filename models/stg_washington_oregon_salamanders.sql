{{
    config(
        materialized = 'table'
    )
}}

with base as (
    SELECT *
    FROM {{source('datacaudata', 'washington_oregon_salamanders')}}
)

SELECT *
FROM base