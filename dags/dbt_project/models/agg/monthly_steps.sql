{{
    config(
      materialized='incremental',
    )
}}

SELECT
    device_id,
    "month",
    SUM(step_count) AS step_count
FROM stage.steps
-- Optional incremental run
{% if is_incremental() %}
WHERE "month" > '{{ get_max_insert_date_string("month") }}'
{% endif %}
---------------------------
GROUP BY "month", device_id
ORDER BY "month" DESC, device_id DESC