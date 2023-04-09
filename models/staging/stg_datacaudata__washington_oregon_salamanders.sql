with base as (
    SELECT 
		 id,
         inaturalist_id,
	     taxon_id,
	     name as latin_name,
	     preferred_common_name,
	     DATE_TRUNC('day', observed_date) as date_day_observed,
	     observed_month::INTEGER as observed_month,
	     observed_hour::INTEGER as observed_hour,
	     observed_week::INTEGER as observed_week,
	     observed_year::INTEGER as observed_year,
	     observed_day::INTEGER as observed_day,
		 updated_at,
	     species_guess,
	     (species_guess = preferred_common_name) as species_guess_correct,
	     place_ids::INTEGER[] as place_id_array,
         STRING_TO_ARRAY(location, ',')::NUMERIC[] as coordinates,
	     endemic,
	     native,
	     introduced,
	     threatened,
	     rank,
	     wikipedia_url,
		 _data_loaded_date
    FROM {{source('datacaudata', 'washington_oregon_salamanders')}}
    WHERE observed_date >= '2008-01-01'
	  AND observed_date IS NOT NULL
)

SELECT 
     *, 
     coordinates[1] as latitude,
     coordinates[2] as longitude,
     -- Coordinates rounded to 2 decimals give us an accuracy of 1.11 km (http://wiki.gis.com/wiki/index.php/Decimal_degrees)
     {{dbt_utils.generate_surrogate_key(['ROUND(coordinates[1], 2)', 'ROUND(coordinates[2], 2)']
)}} as location_id
FROM base