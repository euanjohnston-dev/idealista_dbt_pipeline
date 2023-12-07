{{ config(materialized='table') }}

WITH 

property_source_main AS (

    SELECT * FROM {{ ref("stg_property_analytics") }}

),

pagination_metadata as (

    SELECT * FROM {{ ref("stg_pagination_metadata") }}

),

load_metadata as (

    SELECT * FROM {{ ref("stg_load_metadata") }}

),

ranked_data as (

    SELECT psm.*,
           lm.dlt_scrape_date,
           ROW_NUMBER() OVER (PARTITION BY lm.dlt_scrape_date, psm.property_code ORDER BY lm.dlt_scrape_date DESC) AS row_num
    FROM property_source_main psm
    LEFT JOIN pagination_metadata pm  
    ON psm._dlt_parent_id = pm._dlt_id
    LEFT JOIN load_metadata lm 
    ON pm._dlt_load_id = lm.load_id
),

final as (

SELECT
  * Except(row_num, dbt_loaded_at_utc, dbt_job_id),
  CURRENT_DATETIME() AS dbt_loaded_at_utc,
  '{{ var("job_id") }}' AS dbt_job_id
FROM
  ranked_data
WHERE
  row_num = 1 AND dlt_scrape_date IS NOT NULL)

SELECT * FROM final