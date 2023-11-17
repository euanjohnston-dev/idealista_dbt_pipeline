{{ config(materialized='table') }}

select  fpu.* EXCEPT(dbt_loaded_at_utc,dbt_job_id),
       CURRENT_DATETIME() AS dbt_loaded_at_utc,
       '{{ var("job_id") }}' AS dbt_job_id
FROM {{ ref("fct_property_unique") }} fpu
where date_of_scrape = (select max(date_of_scrape) from {{ ref("fct_property_unique") }} )