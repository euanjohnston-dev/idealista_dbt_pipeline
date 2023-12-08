{{ config(materialized='table') }}

SELECT *
FROM {{ ref("rpt_unique_properties") }}
WHERE dlt_scrape_date = (SELECT max(dlt_scrape_date) FROM {{ ref("rpt_unique_properties") }})

