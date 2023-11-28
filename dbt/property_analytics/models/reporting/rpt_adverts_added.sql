{{ config(materialized='table') }}


WITH LatestScrapeDates AS (
  SELECT
    property_code,
    Min(dlt_scrape_date) AS dlt_scrape_date
  FROM {{ ref("fct_properties") }}
  GROUP BY property_code
)

SELECT
  ls.property_code,
  ls.dlt_scrape_date,
  fpu.price,
  fpu.size,
  fpu.rooms,
  fpu.bathrooms,
  fpu.price_by_area,
  CURRENT_DATETIME() AS dbt_loaded_at_utc,
  '{{ var("job_id") }}' AS dbt_job_id
FROM LatestScrapeDates ls
JOIN {{ ref("fct_properties") }}  fpu ON ls.property_code = fpu.property_code AND ls.dlt_scrape_date = fpu.dlt_scrape_date
WHERE ls.dlt_scrape_date != (SELECT Min(dlt_scrape_date) FROM {{ ref("fct_properties") }})