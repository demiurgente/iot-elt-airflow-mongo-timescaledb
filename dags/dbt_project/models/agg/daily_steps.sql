{{
    config(
      materialized='incremental',
    )
}}

SELECT
    device_id,
    "day",
    SUM(step_count) AS step_count
FROM stage.steps
-- Optional incremental run
{% if is_incremental() %}
WHERE "day" > '{{ get_max_insert_date_string("day") }}'
{% endif %}
---------------------------
GROUP BY "day", device_id
ORDER BY "day" DESC, device_id DESC