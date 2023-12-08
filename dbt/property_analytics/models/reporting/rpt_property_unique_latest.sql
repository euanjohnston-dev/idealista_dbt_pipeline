{{ config(materialized='table') }}


WITH 

property_duplicates AS (

    SELECT * FROM {{ ref('dim_property_duplicates') }}

),

properties AS (

    SELECT * FROM {{ ref('fct_properties') }}

),


active_listings as (

SELECT duplicate_group_id, 
       MAX(duplicate_property_code) AS duplicate_property_code, 
       COUNT(*) AS live_listings
FROM property_duplicates
where advert_live = true
GROUP BY duplicate_group_id
),

total_listings AS (
SELECT duplicate_group_id, 
       MAX(duplicate_property_code) AS duplicate_property_code, 
       COUNT(*) AS all_time_listings
FROM property_duplicates
GROUP BY duplicate_group_id),

duplicate_listings_final AS (
SELECT al.*,
       tl.all_time_listings
FROM active_listings al
LEFT JOIN total_listings tl ON al.duplicate_property_code = tl.duplicate_property_code),

duplicates_to_remove AS (
SELECT duplicate_group_id, 
       MAX(duplicate_property_code) AS duplicate_property_code, 
       COUNT(*) AS live_listings
FROM property_duplicates
GROUP BY duplicate_group_id ),

duplicates_to_remove_final AS (

SELECT duplicate_property_code 
FROM property_duplicates 
WHERE duplicate_property_code 
NOT IN (SELECT duplicate_property_code FROM duplicates_to_remove))

SELECT spa.* EXCEPT(dbt_loaded_at_utc,dbt_job_id),
       CASE WHEN dlf.duplicate_group_id IS NULL THEN '-' ELSE dlf.duplicate_group_id END AS duplicate_group_id,
       CASE WHEN dlf.live_listings IS NULL THEN 1 ELSE dlf.live_listings END AS live_listings,
       CASE WHEN dlf.all_time_listings IS NULL THEN 1 ELSE dlf.all_time_listings END AS all_time_listings,
       CURRENT_DATETIME() AS dbt_loaded_at_utc,
       '{{ var("job_id") }}' AS dbt_job_id
FROM  properties  spa
LEFT JOIN duplicate_listings_final dlf ON spa.property_code = dlf.duplicate_property_code
WHERE dlt_scrape_date = (SELECT MAX(dlt_scrape_date) FROM properties) AND property_code NOT IN (SELECT *
FROM duplicates_to_remove_final)

