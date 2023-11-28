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
    LOWER(TO_HEX(SHA256(CONCAT(CAST(property_code AS STRING), '_', CAST(duplicate_group_id AS STRING))))) AS ID,
    property_code,
    duplicate_group_id
  FROM {{ source('sheets_check', 'upload_duplicates') }}
)

SELECT
  Distinct 
  ID,
  CAST(property_code AS STRING) as property_code,
  CAST(duplicate_group_id AS STRING) as duplicate_group_id,
  CURRENT_DATETIME() AS dbt_loaded_at_utc,
  '{{ var("job_id") }}' AS dbt_job_id
FROM table_base