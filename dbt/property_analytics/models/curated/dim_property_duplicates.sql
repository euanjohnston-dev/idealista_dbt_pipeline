

WITH property_duplicates AS (

  SELECT * FROM {{ ref("int_property_duplicates") }} ipd

),

properties AS (

  SELECT * FROM {{ ref("fct_properties") }} fp

),

get_latest_property_ids AS (

    SELECT property_code
    FROM properties
    WHERE dlt_scrape_date = (
      SELECT MAX(dlt_scrape_date)
      FROM properties
    )

),

get_latest_properties AS (

  SELECT
    pd.ID,
    pd.property_code AS duplicate_property_code,
    glpi.property_code AS latest_property_code,
    pd.duplicate_group_id
  FROM property_duplicates pd
  LEFT JOIN get_latest_property_ids glpi 
  ON pd.property_code = glpi.property_code

),

get_max_and_min_dates AS (

  SELECT property_code, 
         min(dlt_scrape_date) as first_scrape_date,
         max(dlt_scrape_date) as latest_scrape_date
  FROM  {{ ref("fct_properties") }}
  GROUP BY property_code

),

final as (

  SELECT
    glp.ID,
    glp.duplicate_property_code,
    glp.duplicate_group_id,
    COALESCE(glp.latest_property_code IS NOT NULL, false) AS advert_live,
    gmamd.first_scrape_date,
    gmamd.latest_scrape_date,
    CURRENT_DATETIME() AS dbt_loaded_at_utc,
    '{{ var("job_id") }}' AS dbt_job_id
  FROM get_latest_properties glp
  LEFT JOIN get_max_and_min_dates gmamd ON  glp.duplicate_property_code = gmamd.property_code

)

SELECT * FROM final
