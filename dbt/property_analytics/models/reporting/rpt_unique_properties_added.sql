

WITH LatestScrapeDates AS (
  SELECT
    property_code,
    Min(dlt_scrape_date) AS dlt_scrape_date
  FROM {{ ref("fct_properties") }}
  GROUP BY property_code
),

properties_added as (
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
WHERE ls.dlt_scrape_date != (SELECT Min(dlt_scrape_date) FROM {{ ref("fct_properties") }}))
,
join_duplicates AS (
  SELECT
    rpa.*,
    dpa.duplicate_group_id
  FROM properties_added rpa 
  LEFT JOIN {{ ref("dim_property_duplicates") }}  dpa ON rpa.property_code = dpa.duplicate_property_code
),
live_duplicates AS (
  SELECT
    duplicate_group_id,
    MIN(first_scrape_date) AS first_scrape_date,
    COUNT(*) AS total_duplicate_adverts
  FROM {{ ref("dim_property_duplicates") }}
  WHERE duplicate_group_id IS NOT NULL
  GROUP BY duplicate_group_id
)

SELECT
  jd.*
FROM join_duplicates jd
LEFT JOIN live_duplicates ON jd.duplicate_group_id = live_duplicates.duplicate_group_id
LEFT JOIN (
  SELECT
    jd.duplicate_group_id,
    COUNT(*) OVER (PARTITION BY jd.duplicate_group_id) AS duplicate_adverts_added_count
  FROM join_duplicates jd
) counts ON jd.duplicate_group_id = counts.duplicate_group_id
WHERE jd.duplicate_group_id IS NULL
   OR (live_duplicates.total_duplicate_adverts = counts.duplicate_adverts_added_count AND jd.dlt_scrape_date = live_duplicates.first_scrape_date)

