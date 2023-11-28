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
  WHERE dlt_scrape_date = (
    SELECT MAX(dlt_scrape_date)
    FROM {{ ref("stg_property_analytics") }}
  )
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
    CASE WHEN a.contact_info__microsite_short_name IS NULL THEN '-' ELSE a.contact_info__microsite_short_name END AS first_agency,
    CASE WHEN b.contact_info__microsite_short_name IS NULL THEN '-' ELSE b.contact_info__microsite_short_name END AS second_agency,
    a.description AS first_description,
    b.description AS second_description,
    6371 * 2 * ASIN(SQRT(
      POWER(SIN(0.017453292519943295 * (a.latitude - b.latitude) / 2), 2) +
      COS(0.017453292519943295 * a.latitude) * COS(0.017453292519943295 * b.latitude) * POWER(SIN(0.017453292519943295 * (a.longitude - b.longitude) / 2), 2)
    )) AS distance
  FROM PropertyData AS a
  JOIN PropertyData AS b
    ON a.property_code < b.property_code
  ORDER BY a.size_str, a.rooms_str, a.bathrooms_str, a.property_code, b.property_code
),

Duplicate_pairs AS (
  SELECT
    p1.property_code AS property_code_1,
    p2.property_code AS property_code_2
  FROM
    {{ ref("stg_property_duplicates") }} p1
  JOIN
    {{ ref("stg_property_duplicates") }} p2
  ON
    p1.duplicate_group_id = p2.duplicate_group_id
    AND p1.property_code != p2.property_code
),
get_associated_duplicates as (

SELECT first_duplicate, 
       second_duplicate,
       dpd1.duplicate_group_id as first_duplicate_group_id,
       dpd2.duplicate_group_id as second_duplicate_group_id
FROM Differentials d
LEFT JOIN {{ ref("stg_property_distinct") }} pdi 
  ON (d.first_duplicate = CAST(pdi.property_code_1 AS STRING) AND d.second_duplicate = pdi.property_code_2)
      OR (d.first_duplicate = CAST(pdi.property_code_2 AS STRING) AND d.second_duplicate = pdi.property_code_1)
LEFT JOIN {{ ref("stg_property_duplicates") }} dpd1 ON d.first_duplicate = dpd1.property_code
LEFT JOIN {{ ref("stg_property_duplicates") }} dpd2 ON d.second_duplicate = dpd2.property_code
WHERE 
  d.distance < 1
  AND d.first_agency != d.second_agency
  AND d.first_price = d.second_price
  AND pdi.property_code_1 IS NULL
  AND NOT EXISTS (
    SELECT 1
    FROM Duplicate_pairs dp
    WHERE (dp.property_code_1 = d.first_duplicate AND dp.property_code_2 = d.second_duplicate)
       OR (dp.property_code_1 = d.second_duplicate AND dp.property_code_2 = d.first_duplicate)
  )
), 
RankedDuplicates as (
SELECT
  first_duplicate,
  second_duplicate,
  first_duplicate_group_id,
  second_duplicate_group_id,
  ROW_NUMBER() OVER (PARTITION BY first_duplicate_group_id ORDER BY first_duplicate_group_id) AS rn1,
  ROW_NUMBER() OVER (PARTITION BY second_duplicate_group_id ORDER BY second_duplicate_group_id) AS rn2
FROM get_associated_duplicates)

SELECT
  first_duplicate,
  second_duplicate,
  first_duplicate_group_id,
  second_duplicate_group_id,
  CURRENT_DATETIME() AS dbt_loaded_at_utc,
  '{{ var("job_id") }}' AS dbt_job_id
FROM RankedDuplicates
WHERE rn1 = 1 
   OR rn2 = 1 
   OR first_duplicate_group_id IS NULL AND  second_duplicate_group_iD IS NULL
