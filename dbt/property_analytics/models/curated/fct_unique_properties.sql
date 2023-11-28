{{ config(materialized='table') }}

WITH duplicate_group_assignation AS (
  SELECT
    spa.*,
    dpd.duplicate_group_id,
    ROW_NUMBER() OVER (PARTITION BY dpd.duplicate_group_id, spa.dlt_scrape_date ORDER BY property_code) AS row_num
  FROM
    {{ ref("stg_property_analytics") }}  spa
    LEFT JOIN {{ ref("dim_property_duplicates") }}  dpd ON spa.property_code = dpd.duplicate_property_code
)
SELECT * except(duplicate_group_id,row_num)
FROM duplicate_group_assignation
WHERE row_num = 1 OR duplicate_group_id IS NULL