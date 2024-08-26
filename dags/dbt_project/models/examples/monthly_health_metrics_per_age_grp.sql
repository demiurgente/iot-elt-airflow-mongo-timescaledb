WITH user_age_grps AS (
	SELECT
	    user_id,
	    devices,
			CASE
	        WHEN "age" < 18 THEN 'Under 18'
	        WHEN "age" BETWEEN 18 AND 24 THEN '18-24'
	        WHEN "age" BETWEEN 25 AND 34 THEN '25-34'
	        WHEN "age" BETWEEN 35 AND 44 THEN '35-44'
	        WHEN "age" BETWEEN 45 AND 54 THEN '45-54'
	        WHEN "age" BETWEEN 55 AND 64 THEN '55-64'
	        WHEN "age" > 64 THEN 'Above 64'
	 		END AS age_group
	FROM stage.users
  WHERE "age" BETWEEN 5 AND 130
)

SELECT
    "month",
		u.age_group,
    AVG(ds.avg_sleep_bpm) AS avg_sleep_bpm,
    AVG(ds.step_count) AS avg_step_count
FROM agg.monthly_summary ds
JOIN user_age_grps u
  ON ds.device_id = ANY(u.devices)
GROUP BY "month", u.age_group
ORDER BY "month" DESC, "age_group" DESC