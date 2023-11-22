{{ config(materialized='table') }}

select ID,
       parent_property,
       child_property,
       CURRENT_DATETIME() AS dbt_loaded_at_utc,
       '{{ var("job_id") }}' AS dbt_job_id
FROM {{ ref("stg_property_duplicates") }} spd
