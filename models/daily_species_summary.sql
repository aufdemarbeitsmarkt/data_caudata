with salamanders as (
    SELECT *
    FROM {{ref('stg_washington_oregon_salamanders')}}
    WHERE rank IN ('species', 'subspecies')
), 

days as (
    SELECT *
    FROM {{ref('dates')}}
),

species_days as (
    SELECT DISTINCT 
         days.date_day as date_day_observed,
         salamanders.taxon_id,
         salamanders.latin_name,
         salamanders.preferred_common_name
    FROM days
    CROSS JOIN salamanders
),

daily_summary as (
    SELECT
         species_days.date_day_observed,
         species_days.taxon_id,
         species_days.latin_name,
         species_days.preferred_common_name, 
         COALESCE(COUNT(DISTINCT salamanders.inaturalist_id), 0) as num_observations,
        -- place_ids (probably need a places model)
        -- day over day change
        -- avg rolling day over day change
    FROM species_days 
    LEFT JOIN salamanders
        ON salamanders.date_day_observed = species_days.date_day_observed AND salamanders.taxon_id = species_days.taxon_id
    GROUP BY 1,2,3,4
)

SELECT *
FROM daily_summary
WHERE date_day_observed < CURRENT_DATE
ORDER BY date_day_observed