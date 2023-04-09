with monthly_species_summary_by_location as (
    SELECT 
        date_month_observed as ds,
        species_location_id as unique_id, 
        num_observations as y,
        SUM(num_observations) OVER (PARTITION BY species_location_id) as num_observations_this_species_location_id
    FROM {{ref('monthly_species_summary_by_location')}}
    WHERE EXTRACT('year' FROM date_month_observed) >= EXTRACT('year' FROM CURRENT_DATE) - 3
)

SELECT 
     ds,
     unique_id,
     y
FROM monthly_species_summary_by_location
-- to be eligible for forecasting, let's require 5 observations for a given location and species combination in the last 3 years
WHERE num_observations_this_species_location_id > 5
