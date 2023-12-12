-- models/my_schema/my_incremental_model.sql


WITH upload_duplicates AS (

  SELECT DISTINCT *
  FROM {{ ref("stg_duplicate_processing_additional_duplicates") }} sad

),

additional_duplicates AS (

  SELECT DISTINCT *
  FROM {{ ref("stg_sheets_check_upload_duplicates") }} sad

),

unioned AS (

  SELECT * EXCEPT(dbt_loaded_at_utc, dbt_job_id)
  FROM upload_duplicates
  UNION ALL 
  SELECT * EXCEPT(dbt_loaded_at_utc, dbt_job_id)
  FROM additional_duplicates

),

final as (

  SELECT  ID,
          property_code,
          duplicate_group_id,
          _dlt_load_id,
          _dlt_id,
          CURRENT_DATETIME() AS dbt_loaded_at_utc,
          '{{ var("job_id") }}' AS dbt_job_id
  FROM unioned
)

SELECT * FROM final