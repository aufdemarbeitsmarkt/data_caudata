with salamanders as (
    SELECT *
    FROM {{ref('stg_datacaudata__washington_oregon_salamanders')}}
),

taxon_to_place as (
    SELECT DISTINCT
         date_day_observed,
         taxon_id,
         UNNEST(place_id_array) as place_id
    FROM salamanders
)

SELECT *
FROM taxon_to_place