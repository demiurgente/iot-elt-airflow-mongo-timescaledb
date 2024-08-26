{{
    config(
      order_by='device_id',
      materialized='incremental'
    )
}}

SELECT
    r.device_id,
    TO_TIMESTAMP(rm.ts) AS created_at,
    TO_CHAR(TO_TIMESTAMP(rm.ts),'YYYYMM') AS "month",
    TO_CHAR(TO_TIMESTAMP(rm.ts),'YYYYMMDD') AS "day",
    rm.bpm,
    rm.confidence
FROM {{ source('raw','heart_rates') }} r
JOIN {{ source('raw','heart_rates__metrics') }} rm ON 1=1
-- Optional incremental run
{% if is_incremental() %}
  AND r.created_at > {{ get_max_insert_timestamp('created_at') }}
{% endif %}
---------------------------
  AND r._dlt_id = rm._dlt_parent_id
