{{ config(materialized='table') }}

WITH RECURSIVE ParentChild AS (
  SELECT
    property_code_1 AS parent,
    property_code_2 AS child,
    0 AS level
    FROM {{ ref("dim_property_duplicates") }} d

  UNION ALL
  SELECT
    p.parent,
    d.property_code_2 AS child,
    p.level + 1
  FROM
    ParentChild p
  JOIN
    {{ ref("dim_property_duplicates") }} d
  ON
    p.child = d.property_code_1
), 
duplicate_relationships as (

SELECT distinct
  parent,
  child
FROM
  ParentChild -- To exclude self-referencing duplicates
ORDER BY
  parent, child
),
 ParentChildAggregated AS (
  SELECT
    parent AS id,
    STRING_AGG(child, ', ') AS duplicate_listings,
    COUNT(child) + 1 AS total_listings
  FROM
    duplicate_relationships
  GROUP BY
    parent
)
SELECT
  mt.* EXCEPT(dbt_loaded_at_utc,dbt_job_id),
  COALESCE(pca.duplicate_listings, '') AS duplicate_listings,
  COALESCE(pca.total_listings, 1) AS total_listings,
  cast(inserted_at as Date) as date_of_scrape,#
  CURRENT_DATETIME() AS dbt_loaded_at_utc,
  '{{ var("job_id") }}' AS dbt_job_id
FROM
 {{ ref("stg_property_analytics") }} AS mt
LEFT JOIN
  ParentChildAggregated AS pca
ON
  mt.property_code = pca.id
left join {{ ref("stg_pagination_metadata") }} par on mt._dlt_parent_id = par._dlt_id
left join {{ ref("stg_load_metadata") }} dl on par._dlt_load_id = dl.load_id
WHERE
  mt.property_code NOT IN (
    SELECT child
    FROM duplicate_relationships
  )
  AND mt.property_code IN (
    SELECT parent_id
    FROM (
      SELECT
        mt.property_code AS parent_id
      FROM
        {{ ref("stg_property_analytics") }} AS mt
      LEFT JOIN
        ParentChildAggregated AS pca
      ON
        mt.property_code = pca.id
      WHERE
        mt.property_code NOT IN (
          SELECT child
          FROM duplicate_relationships
        )
    )
  )