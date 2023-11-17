{{ config(materialized='table') }}


WITH DistinctDates AS (
  SELECT DISTINCT date_of_scrape
  FROM {{ ref("fct_property_unique") }}
)

SELECT
  dd.date_of_scrape,
  COALESCE(ca.count_added, 0) AS properties_added,
  COALESCE(cr.count_removed, 0) AS properties_removed,
  CURRENT_DATETIME() AS dbt_loaded_at_utc,
  '{{ var("job_id") }}' AS dbt_job_id
FROM DistinctDates dd
LEFT JOIN (
  SELECT date_of_scrape, COUNT(*) AS count_removed
  FROM {{ ref("rpt_properties_removed") }} 
  GROUP BY date_of_scrape
) cr ON dd.date_of_scrape = cr.date_of_scrape
LEFT JOIN (
  SELECT date_of_scrape, COUNT(*) AS count_added
  FROM {{ ref("rpt_properties_added") }} 
  GROUP BY date_of_scrape
) ca ON dd.date_of_scrape = ca.date_of_scrape
ORDER BY dd.date_of_scrape DESC