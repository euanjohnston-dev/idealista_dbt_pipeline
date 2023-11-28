{{ config(materialized='table') }}

select *
FROM {{ ref("stg_property_analytics") }} spd
