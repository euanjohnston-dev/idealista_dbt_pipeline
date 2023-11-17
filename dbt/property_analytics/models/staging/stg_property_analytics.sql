{{ config(materialized='table') }}

select *, 
       CURRENT_DATETIME() AS dbt_loaded_at_utc,
       '{{ var("job_id") }}' AS dbt_job_id
FROM {{ source('property_analytics', 'property_source_main') }}