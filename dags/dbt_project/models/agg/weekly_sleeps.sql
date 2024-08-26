{{
    config(
      materialized='incremental',
    )
}}

SELECT
    device_id,
    time_bucket('1 week', created_at) AS "week",
    avg(bpm) AS avg_sleep_bpm
FROM stage.heart_rates hr
-- Optional incremental run
{% if is_incremental() %}
WHERE "week" > '{{ get_max_insert_date_string("week") }}'
{% endif %}
---------------------------
GROUP BY "week", device_id
ORDER BY "week" DESC, device_id DESC