WITH RECURSIVE ParentChild AS (
  SELECT
    parent_property AS parent,
    child_property AS child,
    0 AS level
  FROM {{ ref("dim_property_duplicates") }} d

  UNION ALL

  SELECT
    p.parent,
    d.child_property AS child,
    p.level + 1
  FROM
    ParentChild p
  JOIN
     {{ ref("dim_property_duplicates") }} d
  ON
    p.child = d.parent_property
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
  mt.* EXCEPT(dbt_loaded_at_utc),
  COALESCE(pca.duplicate_listings, '') AS duplicate_listings,
  COALESCE(pca.total_listings, 1) AS total_listings,
  CAST(inserted_at AS Date) AS date_of_scrape,
  CURRENT_DATETIME() AS dbt_loaded_at_utc
FROM
  {{ ref("stg_property_analytics") }} AS mt
LEFT JOIN
  ParentChildAggregated AS pca
ON
  mt.property_code = CAST(pca.id AS STRING)
LEFT JOIN
  {{ ref("stg_pagination_metadata") }} par
ON
  mt._dlt_parent_id = par._dlt_id
LEFT JOIN
  {{ ref("stg_load_metadata") }} dl
ON
  par._dlt_load_id = dl.load_id
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
        mt.property_code = CAST(pca.id AS STRING)
      WHERE
        mt.property_code NOT IN (
          SELECT child
          FROM duplicate_relationships
        )
    )
  )
