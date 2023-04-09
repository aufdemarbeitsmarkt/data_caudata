with weekly_summary as (
    SELECT 
        taxon_id as unique_id, 
        DATE_TRUNC('week', date_day_observed + INTERVAL '1 day') - INTERVAL '1 day' as ds,
        SUM(num_observations) as y
    FROM {{ref('daily_species_summary')}}
    GROUP BY 1,2
),

summary_with_lag as (
    SELECT 
         *,
         LAG(y, 52) OVER (PARTITION BY unique_id ORDER BY ds) as one_year_ago_y
    FROM weekly_summary
)

SELECT 
     *,
     (y - one_year_ago_y)::DOUBLE PRECISION / COALESCE(NULLIF(one_year_ago_y, 0), 1)::DOUBLE PRECISION as yoy_delta
FROM summary_with_lag
