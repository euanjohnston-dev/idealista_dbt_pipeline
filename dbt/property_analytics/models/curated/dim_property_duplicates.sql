{{ config(materialized='table') }}

select ID,
       property_code_1,
       property_code_2,
       CURRENT_DATETIME() AS dbt_loaded_at_utc,
       '{{ var("job_id") }}' AS dbt_job_id
FROM {{ ref("stg_property_duplicates") }} spd
