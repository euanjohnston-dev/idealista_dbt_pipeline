{{ config(materialized='table') }}

WITH join_latest AS (
  SELECT
    spd.ID,
    spd.property_code AS duplicate_property_code,
    spa.property_code AS latest_property_code,
    spd.duplicate_group_id,
    CURRENT_DATETIME() AS dbt_loaded_at_utc,
    '{{ var("job_id") }}' AS dbt_job_id
  FROM {{ ref("stg_property_duplicates") }} spd
  LEFT JOIN (
    SELECT property_code
    FROM {{ ref("stg_property_analytics") }}
    WHERE dlt_scrape_date = (
      SELECT MAX(dlt_scrape_date)
      FROM {{ ref("stg_property_analytics") }}
    )
  ) spa ON spd.property_code = spa.property_code
)

SELECT
  jl.ID,
  jl.duplicate_property_code,
  jl.duplicate_group_id,
  COALESCE(jl.latest_property_code IS NOT NULL, false) AS advert_live,
  spa.first_scrape_date,
  spa.latest_scrape_date,
  jl.dbt_loaded_at_utc,
  jl.dbt_job_id
FROM join_latest jl
left join (select property_code, 
                  min(dlt_scrape_date) as first_scrape_date,
                  max(dlt_scrape_date) as latest_scrape_date
                  from  {{ ref("stg_property_analytics") }}
                  group by property_code) spa on jl.duplicate_property_code = spa.property_code