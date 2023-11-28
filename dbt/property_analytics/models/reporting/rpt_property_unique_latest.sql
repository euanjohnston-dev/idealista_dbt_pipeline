{{ config(materialized='table') }}

with active_listings as (

SELECT duplicate_group_id, 
       MAX(duplicate_property_code) AS duplicate_property_code, 
       COUNT(*) AS live_listings
FROM {{ ref('dim_property_duplicates') }}
where advert_live = true
GROUP BY duplicate_group_id
),

total_listings as (
SELECT duplicate_group_id, 
       MAX(duplicate_property_code) AS duplicate_property_code, 
       COUNT(*) AS all_time_listings
FROM {{ ref('dim_property_duplicates') }}
GROUP BY duplicate_group_id),
duplicate_listings_final as (
select al.*,
       tl.all_time_listings
from active_listings al
left join total_listings tl on al.duplicate_property_code = tl.duplicate_property_code),

duplicates_to_remove_1 as (
SELECT duplicate_group_id, 
       MAX(duplicate_property_code) AS duplicate_property_code, 
       COUNT(*) AS live_listings
FROM {{ ref('dim_property_duplicates') }}
group by duplicate_group_id ),

duplicates_to_remove_final as (

select duplicate_property_code from {{ ref('dim_property_duplicates') }} where duplicate_property_code not in (select duplicate_property_code from duplicates_to_remove_1 ))

select spa.* EXCEPT(dbt_loaded_at_utc,dbt_job_id),
       case when dlf.duplicate_group_id is null then '-' else dlf.duplicate_group_id end as duplicate_group_id,
       case when dlf.live_listings is null then 1 else dlf.live_listings end as live_listings,
       case when dlf.all_time_listings is null then 1 else dlf.all_time_listings end as all_time_listings,
       CURRENT_DATETIME() AS dbt_loaded_at_utc,
       '{{ var("job_id") }}' AS dbt_job_id
from  {{ ref('stg_property_analytics') }}  spa
left join duplicate_listings_final dlf on spa.property_code = dlf.duplicate_property_code
where dlt_scrape_date = (select max(dlt_scrape_date) from {{ ref('stg_property_analytics') }}) and property_code not in (select *
from duplicates_to_remove_final)
