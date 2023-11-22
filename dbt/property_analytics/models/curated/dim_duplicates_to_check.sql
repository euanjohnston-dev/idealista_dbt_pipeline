{{ config(materialized='table') }}

WITH PropertyData AS (
  SELECT
    property_code,
    size AS size_str,
    rooms AS rooms_str,
    bathrooms AS bathrooms_str,
    latitude,
    longitude,
    price,
    contact_info__microsite_short_name,
    description
  FROM {{ ref("stg_property_analytics") }} lps
),

Differentials AS (
  SELECT
    a.property_code AS first_duplicate,
    b.property_code AS second_duplicate,
    a.latitude AS first_latitude,
    a.longitude AS first_longitude,
    b.latitude AS second_latitude,
    b.longitude AS second_longitude,
    a.price AS first_price,
    b.price AS second_price,
    a.price - b.price AS price_differential,
    a.contact_info__microsite_short_name as first_agency,
    b.contact_info__microsite_short_name as second_agency,
    a.description as first_description,
    b.description as second_description,
    6371 * 2 * ASIN(SQRT(
      POWER(SIN(0.017453292519943295 * (a.latitude - b.latitude) / 2), 2) +
      COS(0.017453292519943295 * a.latitude) * COS(0.017453292519943295 * b.latitude) * POWER(SIN(0.017453292519943295 * (a.longitude - b.longitude) / 2), 2)
    )) AS distance
  FROM PropertyData AS a
  CROSS JOIN PropertyData AS b
  WHERE  a.property_code < b.property_code
  ORDER BY a.size_str, a.rooms_str, a.bathrooms_str, a.property_code, b.property_code
),

FilteredDifferentials AS (
  SELECT cpd.*
  FROM Differentials cpd
  LEFT JOIN {{ ref("stg_property_duplicates") }} pdu
  ON (cpd.first_duplicate = CAST(pdu.parent_property AS STRING) AND cpd.second_duplicate = pdu.child_property)
      OR (cpd.first_duplicate = CAST(pdu.child_property AS STRING) AND cpd.second_duplicate = pdu.parent_property)
  LEFT JOIN {{ ref("stg_property_distinct") }} pdi 
  ON (cpd.first_duplicate = CAST(pdi.parent_property AS STRING) AND cpd.second_duplicate = pdi.child_property)
      OR (cpd.first_duplicate = CAST(pdi.child_property AS STRING) AND cpd.second_duplicate = pdi.parent_property)
  WHERE pdi.parent_property IS NULL and pdu.parent_property IS NULL
    AND cpd.distance < 1
    AND cpd.first_price = cpd.second_price
    AND cpd.first_agency != cpd.second_agency
), 
EstablishExistingRelationships AS (
  SELECT
    DISTINCT
    fd.first_duplicate AS original_first_duplicate,
    fd.second_duplicate AS original_second_duplicate,
    CASE
      WHEN dpd1.parent_property IS NOT NULL THEN dpd1.parent_property
      WHEN dpd2.parent_property IS NOT NULL THEN dpd2.parent_property
      WHEN dpd5.parent_property IS NOT NULL THEN dpd5.parent_property
      WHEN dpd6.parent_property IS NOT NULL THEN dpd6.parent_property
      ELSE fd.first_duplicate
    END AS parent_duplicate_to_check,
    CASE
      WHEN dpd1.parent_property IS NOT NULL THEN fd.second_duplicate
      WHEN dpd2.parent_property IS NOT NULL THEN fd.first_duplicate
      WHEN dpd5.parent_property IS NOT NULL THEN fd.second_duplicate
      WHEN dpd6.parent_property IS NOT NULL THEN fd.first_duplicate
      ELSE fd.second_duplicate
    END AS child_duplicate_to_check
  FROM
    FilteredDifferentials fd
  LEFT JOIN {{ ref("stg_property_duplicates") }} dpd1
  ON
    fd.first_duplicate = CAST(dpd1.parent_property AS STRING)
  LEFT JOIN {{ ref("stg_property_duplicates") }} dpd2
  ON
    fd.second_duplicate = CAST(dpd2.parent_property AS STRING)
  LEFT JOIN {{ ref("stg_property_duplicates") }} dpd3
  ON
    fd.first_duplicate = CAST(dpd3.child_property AS STRING)
  LEFT JOIN {{ ref("stg_property_duplicates") }} dpd4
  ON
    fd.second_duplicate = CAST(dpd4.child_property AS STRING)
  LEFT JOIN {{ ref("stg_property_duplicates") }} dpd5
  ON
    dpd3.child_property = dpd5.child_property
  LEFT JOIN {{ ref("stg_property_duplicates") }} dpd6
  ON
    dpd4.child_property = dpd6.child_property
  WHERE NOT (dpd3.child_property IS NOT NULL AND dpd4.child_property IS NOT NULL)
)

SELECT Distinct
       parent_duplicate_to_check,
       child_duplicate_to_check,
       CURRENT_DATETIME() AS dbt_loaded_at_utc,
       '{{ var("job_id") }}' AS dbt_job_id
FROM EstablishExistingRelationships
