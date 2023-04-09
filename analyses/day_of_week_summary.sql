with dow as (
	SELECT 
		 EXTRACT('dow' FROM date_day_observed) as int_day_of_week,
		 COUNT(*) as num_observations
	FROM {{ref('stg_datacaudata__washington_oregon_salamanders')}}
	WHERE date_day_observed >= '2020-01-01'
	GROUP BY 1
)

SELECT
	 ('{Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday}'::TEXT[])[int_day_of_week + 1] as _day_of_week,
	 num_observations
FROM dow
ORDER BY int_day_of_week
