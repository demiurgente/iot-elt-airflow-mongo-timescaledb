{{
    config(
      materialized='incremental',
    )
}}

SELECT
    hr.device_id,
    hr.day,
    avg(hr.bpm) AS avg_sleep_bpm
FROM stage.heart_rates hr
JOIN stage.sleeps s ON 1=1
-- Optional incremental run
{% if is_incremental() %}
  AND hr.day > '{{ get_max_insert_date_string("day") }}'
{% endif %}
---------------------------
  AND s.device_id = hr.device_id
  AND s.day = hr.day
GROUP BY hr.day, hr.device_id
ORDER BY hr.day DESC, hr.device_id DESC