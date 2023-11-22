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
  LEFT JOIN  {{ ref("stg_property_duplicates") }}  pdu
  ON (cpd.first_duplicate = pdu.parent_property AND cpd.second_duplicate = pdu.child_property)
      OR (cpd.first_duplicate = pdu.child_property AND cpd.second_duplicate = pdu.parent_property)
  LEFT JOIN  {{ ref("stg_property_distinct") }} pdi 
  ON (cpd.first_duplicate = pdi.parent_property AND cpd.second_duplicate = pdi.child_property)
      OR (cpd.first_duplicate = pdi.child_property AND cpd.second_duplicate = pdi.parent_property)
  WHERE pdi.parent_property IS NULL and pdu.parent_property IS NULL
    AND cpd.distance < 1
    AND cpd.first_price = cpd.second_price
    AND cpd.first_agency != cpd.second_agency
)

SELECT DISTINCT
  CASE
    WHEN dpc.parent_property IS NOT NULL THEN dpc.parent_property
    WHEN dpc2.parent_property IS NOT NULL THEN dpc2.parent_property
  END AS parent_property,
  CASE
    WHEN dpc.parent_property IS NOT NULL THEN dpc.child_property
    WHEN dpc2.parent_property IS NOT NULL THEN dpc2.child_property
  END AS child_property
FROM FilteredDifferentials dct
LEFT JOIN {{ ref("stg_property_duplicates") }} dpc
  ON dct.first_duplicate = dpc.parent_property OR dct.first_duplicate = dpc.child_property
LEFT JOIN {{ ref("stg_property_duplicates") }} dpc2
  ON dct.second_duplicate = dpc2.parent_property OR dct.second_duplicate = dpc2.child_property
WHERE (dpc.parent_property IS NOT NULL AND dpc.child_property IS NOT NULL)
   OR (dpc2.parent_property IS NOT NULL AND dpc2.child_property IS NOT NULL)
