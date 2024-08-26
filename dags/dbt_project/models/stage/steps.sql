{{
    config(
      order_by='device_id',
      materialized='incremental'
    )
}}

SELECT
    s.device_id,
    TO_TIMESTAMP(sm.ts) AS created_at,
    TO_CHAR(TO_TIMESTAMP(created_at),'YYYYMM') AS "month",
    TO_CHAR(TO_TIMESTAMP(created_at),'YYYYMMDD') AS "day",
    sm.steps AS step_count
FROM {{ source('raw','steps') }} s
JOIN {{ source('raw','steps__metrics') }} sm ON 1=1
-- Optional incremental run
{% if is_incremental() %}
  AND s.created_at > {{ get_max_insert_timestamp('created_at') }}
{% endif %}
---------------------------
  AND s._dlt_id = sm._dlt_parent_id
