
WITH source AS (

    SELECT * FROM  {{ source('property_analytics', 'load_metadata') }}

),

transformed as (

    SELECT load_id,
           schema_name,
           status,
           CAST(inserted_at AS DATE) AS dlt_scrape_date,
           schema_version_hash,
           CURRENT_DATETIME() AS dbt_loaded_at_utc,
           '{{ var("job_id") }}' AS dbt_job_id
    FROM source

)

SELECT *
FROM transformed 

stg_load_metadata