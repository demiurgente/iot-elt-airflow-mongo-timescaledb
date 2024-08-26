{{
    config(
      materialized='incremental',
    )
}}

SELECT
    s.device_id,
    s.week,
    s.avg_sleep_bpm,
    ws.step_count
FROM agg.weekly_sleeps s
JOIN agg.weekly_steps ws ON 1=1
-- Optional incremental run
{% if is_incremental() %}
  AND s.week > '{{ get_max_insert_date_string("week") }}'
{% endif %}
---------------------------
  AND s.device_id = ws.device_id
  AND s.week = ws.week
ORDER BY s.week DESC, s.device_id DESC