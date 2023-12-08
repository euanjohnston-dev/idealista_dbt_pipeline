import logging
from dbt_run_pipeline import dbt_run
import base64
from slack_messaging import pipeline_success, pipeline_failure


def dbt_pipeline(data, context):
    """Triggered by a Pub/Sub message."""
    logging.info(f"Function triggered by Pub/Sub event: {data}")

    try:
        # Decode the Pub/Sub message
        message = base64.b64decode(data['data']).decode('utf-8')
        logging.info(f"Received message data: {message}")

        # Add your logic to process the Pub/Sub message if needed

        # Run the DBT command
        
        dbt_run()
        pipeline_success()

        logging.info("DBT command executed successfully.")
    except Exception as e:
        pipeline_failure(e)
        logging.error(f"Error executing DBT command: {e}")