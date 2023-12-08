

WITH distinct_properties AS (
    
    SELECT * EXCEPT (dbt_loaded_at_utc,dbt_job_id)
    FROM {{ ref("stg_sheets_check_property_distinct") }}

),

final AS (
    SELECT ID,
           property_code_1,
           property_code_2,
           _dlt_load_id,
           _dlt_id,
           CURRENT_DATETIME() AS dbt_loaded_at_utc,
           '{{ var("job_id") }}' AS dbt_job_id
    FROM distinct_properties
)

SELECT * FROM final
