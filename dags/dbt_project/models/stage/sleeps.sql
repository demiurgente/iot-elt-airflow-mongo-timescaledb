{{
    config(
      order_by='device_id',
      materialized='incremental'
    )
}}

SELECT
    s.device_id,
    TO_TIMESTAMP(sm.start_ts) AS created_at,
    TO_TIMESTAMP(sm.end_ts) AS ended_at,
    TO_CHAR(TO_TIMESTAMP(sm.start_ts),'YYYYMM') AS "month",
    TO_CHAR(TO_TIMESTAMP(sm.start_ts),'YYYYMMDD') AS "day",
    sm.duration
FROM {{ source('raw','sleeps') }} s
JOIN {{ source('raw','sleeps__metrics') }} sm ON 1=1
-- Optional incremental run
{% if is_incremental() %}
  AND s.created_at > {{ get_max_insert_timestamp('created_at') }}
{% endif %}
---------------------------
  AND s._dlt_id = sm._dlt_parent_id
