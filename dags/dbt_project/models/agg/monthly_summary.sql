{{
    config(
      materialized='incremental',
    )
}}

SELECT
    s.device_id,
    s.month,
    s.avg_sleep_bpm,
    ms.step_count
FROM agg.monthly_sleeps s
JOIN agg.monthly_steps ms ON 1=1
-- Optional incremental run
{% if is_incremental() %}
  AND s.month > '{{ get_max_insert_date_string("month") }}'
{% endif %}
---------------------------
  AND s.device_id = ms.device_id
  AND s.month = ms.month
ORDER BY s.month DESC, s.device_id DESC