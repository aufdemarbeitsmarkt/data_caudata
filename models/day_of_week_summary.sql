with dow as (
	SELECT 
		 EXTRACT(DOW FROM observed_date) as int_day_of_week,
		 COUNT(*) as num_observations
	FROM stg_washington_oregon_salamanders
	WHERE observed_date >= '2020-01-01'
	GROUP BY 1
)

SELECT  		 
	 ('{Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday}'::TEXT[])[int_day_of_week + 1] as _day_of_week,
	 num_observations
FROM dow
ORDER BY int_day_of_week