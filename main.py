import logging
from dbt_run_pipeline import dbt_run
import base64
from dlt.common.runtime.slack import send_slack_message
import os
from dotenv import load_dotenv

load_dotenv()
slack_webhook_url = os.environ.get('SLACK_WEBHOOK_URL')


def notify_on_completion(hook):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                message = f"Function {func.__name__} completed successfully"
                send_slack_message(hook, message)
                return result
            except Exception as e:
                message = f"Function {func.__name__} failed. Error: {str(e)}"
                send_slack_message(hook, message)
                raise
        return wrapper
    return decorator

@notify_on_completion(slack_webhook_url)
def dbt_pipeline(): # data, context
  #  """Triggered by a Pub/Sub message."""
  #  logging.info(f"Function triggered by Pub/Sub event: {data}")

    try:
        # Decode the Pub/Sub message
   #     message = base64.b64decode(data['data']).decode('utf-8')
   #     logging.info(f"Received message data: {message}")

        # Run the DBT command
        dbt_run()

        logging.info("DBT command executed successfully.")
    except Exception as e:
        logging.error(f"Error executing DBT command: {e}")

dbt_pipeline()