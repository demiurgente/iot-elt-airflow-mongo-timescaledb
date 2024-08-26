{{
    config(
      materialized='incremental',
    )
}}

SELECT
    hr.device_id,
    hr.month,
    avg(hr.bpm) AS avg_sleep_bpm
FROM stage.heart_rates hr
JOIN stage.sleeps s ON 1=1
-- Optional incremental run
{% if is_incremental() %}
  AND hr.month > '{{ get_max_insert_date_string("month") }}'
{% endif %}
---------------------------
  AND s.device_id = hr.device_id
  AND s.month = hr.month
GROUP BY hr.month, hr.device_id
ORDER BY hr.month DESC, hr.device_id DESC