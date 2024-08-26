{{
    config(
      materialized='incremental',
    )
}}

SELECT
    s.device_id,
    s.day,
    s.avg_sleep_bpm,
    ds.step_count
FROM agg.daily_sleeps s
JOIN agg.daily_steps ds ON 1=1
-- Optional incremental run
{% if is_incremental() %}
  AND s.day > '{{ get_max_insert_date_string("day") }}'
{% endif %}
---------------------------
  AND s.device_id = ds.device_id
  AND s.day = ds.day
ORDER BY s.day DESC, s.device_id DESC