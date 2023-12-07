{{ config(materialized='table') }}

WITH source AS (

    SELECT * FROM {{ source('property_analytics', 'pagination_metadata') }}

),

transformed as (

    SELECT total,
           total_pages,
           actual_page,
           items_per_page,
           num_paginations,
           filter__location_name,
           alert_name,
           total_applied_filters,
           lower_range_position,
           upper_range_position,
           paginable,
           _dlt_load_id,
           _dlt_id,
           CURRENT_DATETIME() AS dbt_loaded_at_utc,
           '{{ var("job_id") }}' AS dbt_job_id
    FROM source

)

SELECT *
FROM transformed 