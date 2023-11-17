-- models/my_schema/my_incremental_model.sql

-- Configuring the model for incremental materialization
{{config(
  materialized = 'incremental',
  unique_key = 'ID'
)
}}

-- Select statement for the incremental model
WITH table_base AS (
  SELECT
    LOWER(TO_HEX(SHA256(CONCAT(CAST(property_code_1 AS STRING), '_', CAST(property_code_2 AS STRING))))) AS ID,
    property_code_1,
    property_code_2
  FROM {{ source('sheets_check', 'upload_duplicates') }}
)

SELECT 
  ID,
  property_code_1,
  property_code_2,
  CURRENT_DATETIME() AS dbt_loaded_at_utc,
  '{{ var("job_id") }}' AS dbt_job_id
FROM table_base