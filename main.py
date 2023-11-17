import logging
import json
from dbt_run_pipeline import dbt_run

def dbt_pipeline(event, context):
    """Triggered by a Pub/Sub message."""
    logging.info(f"Function triggered by Pub/Sub event: {event}")

    try:
        # Decode the Pub/Sub message
        message_data = json.loads(event["data"])
        logging.info(f"Received message data: {message_data}")

        # Add your logic to process the Pub/Sub message if needed

        # Run the DBT command
        dbt_run()

        logging.info("DBT command executed successfully.")
    except Exception as e:
        logging.error(f"Error executing DBT command: {e}")
