{{ config(materialized='table') }}

WITH ranked_data AS (
  SELECT
    parel.*,
    CAST(inserted_at AS DATE) AS date_,
    ROW_NUMBER() OVER (PARTITION BY CAST(inserted_at AS DATE), property_code ORDER BY inserted_at DESC) AS row_num
FROM {{ source('property_analytics', 'property_source_main') }} parel
  LEFT JOIN
       {{ source('property_analytics', 'pagination_metadata') }} par  ON parel._dlt_parent_id = par._dlt_id
  LEFT JOIN
         {{ source('property_analytics', 'load_metadata') }} dl ON par._dlt_load_id = dl.load_id
)
SELECT
  * Except(date_,row_num, property_code),
  date_ as dlt_scrape_date,
  CAST(property_code AS STRING) AS property_code,
  CURRENT_DATETIME() AS dbt_loaded_at_utc,
  '{{ var("job_id") }}' AS dbt_job_id
FROM
  ranked_data
WHERE
  row_num = 1