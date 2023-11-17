{{ config(materialized='table') }}


WITH LatestScrapeDates AS (
  SELECT
    property_code,
    MAX(date_of_scrape) AS date_of_scrape
  FROM {{ ref("fct_property_unique") }}
  GROUP BY property_code
)

SELECT
  ls.property_code,
  ls.date_of_scrape,
  fpu.price,
  fpu.size,
  fpu.rooms,
  fpu.bathrooms,
  fpu.price_by_area,
  CURRENT_DATETIME() AS dbt_loaded_at_utc,
  '{{ var("job_id") }}' AS dbt_job_id
FROM LatestScrapeDates ls
JOIN {{ ref("fct_property_unique") }}  fpu ON ls.property_code = fpu.property_code AND ls.date_of_scrape = fpu.date_of_scrape
WHERE ls.date_of_scrape != (SELECT MAX(date_of_scrape) FROM {{ ref("fct_property_unique") }})