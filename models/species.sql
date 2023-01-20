{{
    config(
        materialized='table'
    )
}}

with salamanders as (
    SELECT *
    FROM {{ref('stg_datacaudata__washington_oregon_salamanders')}}
    WHERE rank IN ('species', 'subspecies')
), 

distinct_species as (
    SELECT DISTINCT
         taxon_id,
         latin_name,
         preferred_common_name,
         MAX(date_day_observed) as most_recent_observed_date,
         MIN(date_day_observed) as first_observed_date,
         MAX(rank) as rank, 
         MAX(wikipedia_url) as wikipedia_url
    FROM salamanders
    GROUP BY 1,2,3
)

SELECT *
FROM distinct_species
