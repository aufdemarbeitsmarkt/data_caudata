with salamanders as (
    SELECT *
    FROM {{ref('stg_datacaudata__washington_oregon_salamanders')}}
    WHERE rank IN ('species', 'subspecies')
), 

days as (
    SELECT *
    FROM {{ref('dates')}}
    -- let's only care about the last 3 years of observations if dev; 10 for prod
    WHERE EXTRACT('year' FROM date_day) >= (
        EXTRACT('year' FROM CURRENT_DATE) 
        - 
        {{ 3 if target.name == 'dev' else 10 }}
        )
),

species as (
    SELECT 
         taxon_id,
         latin_name,
         preferred_common_name
    FROM {{ref('species')}}
),

species_days as (
    SELECT DISTINCT 
         days.date_day as date_day_observed,
         species.taxon_id,
         species.latin_name,
         species.preferred_common_name
    FROM days
    CROSS JOIN species
),

daily_summary as (
    SELECT
         species_days.date_day_observed,
         species_days.taxon_id,
         species_days.latin_name,
         species_days.preferred_common_name, 
         COALESCE(COUNT(DISTINCT salamanders.inaturalist_id), 0)::INTEGER as num_observations,
         COALESCE(SUM(species_guess_correct::INTEGER), 0)::INTEGER as num_correct_guesses,
         BOOL_OR(endemic) as endemic,
         BOOL_OR(native) as native,
         BOOL_OR(threatened) as threatened
    FROM species_days 
    LEFT JOIN salamanders
        ON salamanders.date_day_observed = species_days.date_day_observed AND salamanders.taxon_id = species_days.taxon_id
    GROUP BY 1,2,3,4
),

windowed as (
    SELECT 
         *,
         LAG(num_observations) OVER (
            PARTITION BY taxon_id 
            ORDER BY date_day_observed
         ) as prior_day_observations,
         LAG(num_observations, 365, 0) OVER (
            PARTITION BY taxon_id
            ORDER BY date_day_observed
         ) as one_year_ago_observations,
         AVG(num_observations) OVER (
            PARTITION BY taxon_id 
            ORDER BY date_day_observed 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
         ) as avg_rolling_seven_day_observations,
         MAX(num_observations) OVER (
            PARTITION BY taxon_id 
            ORDER BY date_day_observed 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
         ) as max_local_seven_day_observations,
         MIN(num_observations) OVER (
            PARTITION BY taxon_id 
            ORDER BY date_day_observed 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
         ) as min_local_seven_day_observations
    FROM daily_summary
),
        
changes_and_deltas as (
    SELECT 
         date_day_observed,
         taxon_id,
         latin_name,
         preferred_common_name,

         num_observations,
         num_correct_guesses,
         prior_day_observations,
         one_year_ago_observations,
         
         (num_observations - prior_day_observations) as dod_observations_change,
         ((num_observations - prior_day_observations) / NULLIF(prior_day_observations, 0)) as  dod_observations_delta,
        
         (num_observations - one_year_ago_observations) as yoy_observations_change,
         ((num_observations - one_year_ago_observations) / NULLIF(one_year_ago_observations, 0)) as  yoy_observations_delta,

         avg_rolling_seven_day_observations,
         max_local_seven_day_observations,
         min_local_seven_day_observations,

         endemic,
         native,
         threatened

    FROM windowed
),

final as (
    SELECT
         date_day_observed,
         taxon_id,
         latin_name,
         preferred_common_name,
         num_observations,
         num_correct_guesses,
         prior_day_observations,
         one_year_ago_observations,
        
         dod_observations_change,
         dod_observations_delta,
       
         yoy_observations_change,
         yoy_observations_delta,
         avg_rolling_seven_day_observations,
         max_local_seven_day_observations,
         min_local_seven_day_observations,

         AVG(dod_observations_change) OVER (
            PARTITION BY taxon_id 
            ORDER BY date_day_observed 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
         ) as avg_rolling_seven_day_dod_observations_change,
         SUM(dod_observations_change) OVER (
            PARTITION BY taxon_id 
            ORDER BY date_day_observed
         ) as cumulative_sum_dod_observations_change,

         BOOL_OR(endemic) OVER (
            PARTITION BY taxon_id
         ) as endemic,
         BOOL_OR(native) OVER (
            PARTITION BY taxon_id
         ) as native,
         BOOL_OR(threatened) OVER (
            PARTITION BY taxon_id
         ) as threatened
    FROM changes_and_deltas
)

SELECT *
FROM final
WHERE date_day_observed < CURRENT_DATE
ORDER BY date_day_observed
