{{ config(materialized='table') }}

select *
FROM {{ ref("stg_property_distinct") }} spd
