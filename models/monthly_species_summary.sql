{{
    config(
        materialized='incremental',
        id='id'
    )
}}

-- TODO: need to cast ints as double precision, especially when dividing

with salamanders as (
    SELECT 
         *,
         DATE_TRUNC('month', date_day_observed) as date_month_observed
    FROM {{ref('stg_datacaudata__washington_oregon_salamanders')}}
    WHERE rank IN ('species', 'subspecies')
), 

months as (
    SELECT *
    FROM {{ref('months')}}
    -- let's only care about the last 3 years of observations if dev; 10 for prod
    WHERE EXTRACT('year' FROM date_month) >= (
        EXTRACT('year' FROM CURRENT_DATE) 
        - 
        {{ 5 if target.name == 'dev' else 10 }}
        )
),

species as (
    SELECT 
         taxon_id,
         latin_name,
         preferred_common_name
    FROM {{ref('species')}}
),

species_months as (
    SELECT DISTINCT 
         months.date_month as date_month_observed,
         species.taxon_id,
         species.latin_name,
         species.preferred_common_name
    FROM months
    CROSS JOIN species
),

monthly_summary as (
    SELECT
         species_months.date_month_observed,
         species_months.taxon_id,
         species_months.latin_name,
         species_months.preferred_common_name, 
         COALESCE(COUNT(DISTINCT salamanders.inaturalist_id), 0)::INTEGER as num_observations,
         COALESCE(SUM(species_guess_correct::INTEGER), 0)::INTEGER as num_correct_guesses
    FROM species_months 
    LEFT JOIN salamanders
        ON salamanders.date_month_observed = species_months.date_month_observed 
        AND salamanders.taxon_id = species_months.taxon_id
    GROUP BY 1,2,3,4
),

windowed as (
    SELECT 
         *,
         LAG(num_observations, 1, 0) OVER (
            PARTITION BY taxon_id 
            ORDER BY date_month_observed
         ) as prior_month_observations,
         LAG(num_observations, 12, 0) OVER (
            PARTITION BY taxon_id
            ORDER BY date_month_observed
         ) as one_year_ago_observations,
         AVG(num_observations) OVER (
            PARTITION BY taxon_id 
            ORDER BY date_month_observed 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
         ) as avg_rolling_three_month_observations,
         MAX(num_observations) OVER (
            PARTITION BY taxon_id 
            ORDER BY date_month_observed 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
         ) as max_local_three_month_observations,
         MIN(num_observations) OVER (
            PARTITION BY taxon_id 
            ORDER BY date_month_observed 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
         ) as min_local_three_month_observations
    FROM monthly_summary
),
        
changes_and_deltas as (
    SELECT 
         date_month_observed,
         taxon_id,
         latin_name,
         preferred_common_name,

         num_observations,
         num_correct_guesses,
         prior_month_observations,
         one_year_ago_observations,
         
         (num_observations - prior_month_observations) as mom_observations_change,
         ((num_observations - prior_month_observations) / NULLIF(prior_month_observations, 0)) as  mom_observations_delta,
        
         (num_observations - one_year_ago_observations) as yoy_observations_change,
         ((num_observations - one_year_ago_observations) / NULLIF(one_year_ago_observations, 0)) as  yoy_observations_delta,

         avg_rolling_three_month_observations,
         max_local_three_month_observations,
         min_local_three_month_observations

    FROM windowed
),

final as (
    SELECT
         date_month_observed,
         taxon_id,
         latin_name,
         preferred_common_name,
         num_observations,
         num_correct_guesses,
         prior_month_observations,
         one_year_ago_observations,
        
         mom_observations_change,
         mom_observations_delta,
       
         yoy_observations_change,
         yoy_observations_delta,
         avg_rolling_three_month_observations,
         max_local_three_month_observations,
         min_local_three_month_observations,

         AVG(mom_observations_change) OVER (
            PARTITION BY taxon_id 
            ORDER BY date_month_observed 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
         ) as avg_rolling_three_month_mom_observations_change,
         SUM(mom_observations_change) OVER (
            PARTITION BY taxon_id 
            ORDER BY date_month_observed
         ) as cumulative_sum_mom_observations_change
    FROM changes_and_deltas
)

SELECT 
     {{dbt_utils.generate_surrogate_key(['taxon_id', 'date_month_observed'])}} as id,
     *
FROM final
WHERE date_month_observed < CURRENT_DATE
ORDER BY date_month_observed
