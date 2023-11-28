{{ config(materialized='table') }}


WITH DistinctDates AS (
  SELECT DISTINCT dlt_scrape_date
  FROM {{ ref("fct_properties") }}
)

SELECT
  dd.dlt_scrape_date,
  COALESCE(ca.count_added, 0) AS properties_added,
  COALESCE(cr.count_removed, 0) AS properties_removed,
  CURRENT_DATETIME() AS dbt_loaded_at_utc,
  '{{ var("job_id") }}' AS dbt_job_id
FROM DistinctDates dd
LEFT JOIN (
  SELECT dlt_scrape_date, COUNT(*) AS count_removed
  FROM {{ ref("rpt_adverts_removed") }} 
  GROUP BY dlt_scrape_date
) cr ON dd.dlt_scrape_date = cr.dlt_scrape_date
LEFT JOIN (
  SELECT dlt_scrape_date, COUNT(*) AS count_added
  FROM {{ ref("rpt_adverts_added") }} 
  GROUP BY dlt_scrape_date
) ca ON dd.dlt_scrape_date = ca.dlt_scrape_date
ORDER BY dd.dlt_scrape_date DESC