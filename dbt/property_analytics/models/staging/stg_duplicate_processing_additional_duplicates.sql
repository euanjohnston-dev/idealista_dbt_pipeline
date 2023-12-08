-- models/my_schema/my_incremental_model.sql


WITH source AS (

  SELECT *
  FROM {{ source('duplicate_processing', 'additional_duplicates') }}

),

transformed AS (


SELECT
  Distinct 
  LOWER(TO_HEX(SHA256(CONCAT(CAST(property_code AS STRING), '_', CAST(duplicate_group_id AS STRING))))) AS ID,
  CAST(property_code AS STRING) as property_code,
  CAST(duplicate_group_id AS STRING) as duplicate_group_id,
  _dlt_load_id,
  _dlt_id,
  CURRENT_DATETIME() AS dbt_loaded_at_utc,
  '{{ var("job_id") }}' AS dbt_job_id
FROM source

)
SELECT * FROM transformed