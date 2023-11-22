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
    LOWER(TO_HEX(SHA256(CONCAT(CAST(parent_property AS STRING), '_', CAST(child_property AS STRING))))) AS ID,
    parent_property,
    child_property
  FROM {{ source('sheets_check', 'upload_duplicates') }}
)

SELECT
  Distinct 
  ID,
  CAST(parent_property AS STRING) as parent_property,
  CAST(child_property AS STRING) as child_property,
  CURRENT_DATETIME() AS dbt_loaded_at_utc,
  '{{ var("job_id") }}' AS dbt_job_id
FROM table_base