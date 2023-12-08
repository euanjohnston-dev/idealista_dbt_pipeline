-- models/my_schema/my_incremental_model.sql



WITH source AS (

    SELECT * FROM {{ source('sheets_check', 'upload_distinct') }}

),

transformed AS (

  SELECT 
    Distinct
    LOWER(TO_HEX(SHA256(CONCAT(CAST(property_code_1 AS STRING), '_', CAST(property_code_2 AS STRING))))) AS ID,
    CAST(property_code_1 AS STRING) as property_code_1,
    CAST(property_code_2 AS STRING) as property_code_2,
    _dlt_load_id,
    _dlt_id,
    CURRENT_DATETIME() AS dbt_loaded_at_utc,
    '{{ var("job_id") }}' AS dbt_job_id
  FROM source

)

SELECT * FROM transformed