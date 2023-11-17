import logging
from dbt_run_pipeline import dbt_run

def pipeline_dbt_run(request): 
    logging.info("Starting Cloud Function execution.")
    try:
        dbt_run()
        logging.info("DBT command executed successfully.")
    except Exception as e:
        logging.error(f"Error executing DBT command: {e}")
