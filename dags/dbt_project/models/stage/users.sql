{{
    config(
      unique_key='user_id',
      materialized='incremental'
    )
}}

SELECT
    u.user_id,
    TO_TIMESTAMP(u.created_at),
    u.age,
    u.height,
    u.gender,
    u.email,
    u.dob,
    ARRAY_AGG(ud.value) as devices
FROM {{ source('raw','users') }} u
LEFT JOIN {{ source('raw','users__devices') }} ud
ON u._dlt_id = ud._dlt_parent_id
GROUP BY u.user_id,
         u.created_at,
         u.age,
         u.height,
         u.gender,
         u.email,
         u.dob