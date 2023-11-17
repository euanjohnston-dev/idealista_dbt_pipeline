{{ config(materialized='table') }}

WITH PropertyData AS (
  SELECT
    property_code,
    CAST(size AS STRING) AS size_str,
    CAST(rooms AS STRING) AS rooms_str,
    CAST(bathrooms AS STRING) AS bathrooms_str,
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
  ON (cpd.first_duplicate = pdu.property_code_1 AND cpd.second_duplicate = pdu.property_code_2)
      OR (cpd.first_duplicate = pdu.property_code_2 AND cpd.second_duplicate = pdu.property_code_1)
  LEFT JOIN  {{ ref("stg_property_distinct") }} pdi 
  ON (cpd.first_duplicate = pdi.property_code_1 AND cpd.second_duplicate = pdi.property_code_2)
      OR (cpd.first_duplicate = pdi.property_code_2 AND cpd.second_duplicate = pdi.property_code_1)
  WHERE pdi.property_code_1 IS NULL and   pdu.property_code_1 IS NULL
    AND cpd.distance < 1
    AND cpd.first_price = cpd.second_price
    AND cpd.first_agency != cpd.second_agency
)

SELECT distinct first_duplicate, second_duplicate
FROM FilteredDifferentials