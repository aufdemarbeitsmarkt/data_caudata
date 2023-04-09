with monthly_species_summary as (
    SELECT 
        date_month_observed as ds,
        taxon_id as unique_id, 
        num_observations as y,
        SUM(num_observations) OVER (PARTITION BY taxon_id) as num_observations_this_taxon_id
    FROM {{ref('monthly_species_summary')}}
    WHERE EXTRACT('year' FROM date_month_observed) >= EXTRACT('year' FROM CURRENT_DATE) - 3
)

SELECT 
     ds,
     unique_id,
     y
FROM monthly_species_summary
-- to be eligible for forecasting, let's require 10 observations for a given taxon ID in the last 3 years
WHERE num_observations_this_taxon_id > 10
