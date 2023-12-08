

WITH properties AS (

  SELECT * FROM {{ ref("fct_properties") }}  fp

),

property_duplicates AS (
  
  SELECT * FROM {{ ref("dim_property_duplicates") }} dpd

),

duplicate_group_assignation AS (

  SELECT
    p.*,
    pd.duplicate_group_id,
    ROW_NUMBER() OVER (PARTITION BY pd.duplicate_group_id, p.dlt_scrape_date ORDER BY p.property_code) AS row_num
  FROM properties p
    LEFT JOIN property_duplicates  pd ON p.property_code = pd.duplicate_property_code

),

filter_logic AS (

  SELECT * EXCEPT(duplicate_group_id,row_num, dbt_loaded_at_utc, dbt_job_id),
         CURRENT_DATETIME() AS dbt_loaded_at_utc,
         '{{ var("job_id") }}' AS dbt_job_id
  FROM duplicate_group_assignation
  WHERE row_num = 1 OR duplicate_group_id IS NULL

),

final AS ( 

  SELECT * from filter_logic

)

SELECT * FROM final