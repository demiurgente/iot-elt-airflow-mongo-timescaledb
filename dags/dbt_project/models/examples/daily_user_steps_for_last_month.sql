WITH user_steps AS (
	-- CTE to aggregate steps across devices
	-- that are associated with the user
	SELECT
	    s.day,
      u.user_id,
  		u.email,
  		u.age,
  		u.gender,
  		s.device_id,
			s.step_count
  FROM agg.daily_steps s
  JOIN stage.users u
    ON s.device_id = ANY(u.devices)
)

SELECT
    "day",
    user_id,
  	email,
  	age,
  	gender,
  	array_agg(device_id) AS devices,
		sum(step_count) AS step_count
FROM user_steps
-- Custom cutoff filter for past month
-- looks up max month, i.e.: '202001'
WHERE "month" = '{{ get_max_insert_date_string("month") }}'
GROUP BY "day",
         user_id,
         email,
		     age,
         gender