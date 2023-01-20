with salamanders as (
    SELECT *
    FROM {{ref('stg_datacaudata__washington_oregon_salamanders')}}
), 

days as (
    SELECT *
    FROM {{ref('dates')}}
),

daily_summary as (
    SELECT 
         date_day_observed,
         COALESCE(COUNT(*), 0) as num_observations,
         COALESCE(COUNT(DISTINCT taxon_id), 0) as num_taxon_observed,
         COALESCE(SUM(species_guess_correct::INTEGER), 0) as num_correct_guesses,
         COALESCE(COUNT(DISTINCT CASE WHEN endemic THEN taxon_id END), 0) as num_endemic_taxon_observed,
         COALESCE(SUM(endemic::INTEGER), 0) as num_endemic_observations,
         COALESCE(COUNT(DISTINCT CASE WHEN native THEN taxon_id END), 0) as num_native_taxon_observed,
         COALESCE(SUM(native::INTEGER), 0) as num_native_observations,
         COALESCE(COUNT(DISTINCT CASE WHEN threatened THEN taxon_id END), 0) as num_threatened_taxon_observed,
         COALESCE(SUM(threatened::INTEGER), 0) as num_threatened_observations
    FROM salamanders   
    GROUP BY 1 
),

final as (
    SELECT
         *,
         num_correct_guesses / NULLIF(num_observations, 0)::DOUBLE PRECISION as pct_observations_guessed_correctly,
         num_endemic_taxon_observed / NULLIF(num_taxon_observed, 0)::DOUBLE PRECISION as pct_taxon_endemic,
         num_native_taxon_observed / NULLIF(num_taxon_observed, 0)::DOUBLE PRECISION as pct_taxon_native,
         num_threatened_taxon_observed / NULLIF(num_taxon_observed, 0)::DOUBLE PRECISION as pct_taxon_threatened
    FROM days 
    LEFT JOIN daily_summary 
        ON daily_summary.date_day_observed = days.date_day
)

SELECT *
FROM final
ORDER BY date_day_observed