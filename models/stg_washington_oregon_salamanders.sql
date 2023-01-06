{{
    config(
        materialized = 'table'
    )
}}

with base as (
    SELECT 
         inaturalist_id,
	     taxon_id,
	     name as latin_name,
	     preferred_common_name,
	     observed_date,
	     observed_month::INTEGER as observed_month,
	     observed_hour::INTEGER as observed_hour,
	     observed_week::INTEGER as observed_week,
	     observed_year::INTEGER as observed_year,
	     observed_day::INTEGER as observed_day,
	     species_guess,
	     (species_guess = preferred_common_name) as      species_guess_correct,
	     identifications_most_disagree,
	     place_ids::INTEGER[] as place_id_array,
	     location as coordinates,
         SPLIT_PART(location, ',', 1)::DOUBLE PRECISION as latitude,
	     SPLIT_PART(location, ',', 2)::DOUBLE PRECISION as longitude,
	     endemic,
	     native,
	     introduced,
	     threatened,
	     rank,
	     wikipedia_url
    FROM {{source('datacaudata', 'washington_oregon_salamanders')}}
)

SELECT *
FROM base