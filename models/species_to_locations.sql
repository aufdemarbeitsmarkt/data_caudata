{{
    config(
        materialized='incremental',
        id='id'
    )
}}

with taxons_locations as (
    SELECT DISTINCT 
         {{dbt_utils.generate_surrogate_key(['taxon_id', 'location_id'])}} as id,
         location_id,
         taxon_id,
         latin_name,
         preferred_common_name,
         MAX(date_day_observed) OVER (PARTITION BY id) as most_recent_observation_date,
         MIN(date_day_observed) OVER (PARTITION BY id) as first_observation_date
    FROM {{ref('stg_datacaudata__washington_oregon_salamanders')}}
)

SELECT *
FROM taxons_locations
