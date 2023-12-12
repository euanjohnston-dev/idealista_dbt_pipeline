import logging
from dbt_run_pipeline import dbt_run
import base64
from dlt.common.runtime.slack import send_slack_message

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

@notify_on_completion("https://hooks.slack.com/services/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")
def dbt_pipeline(data, context): 
    """Triggered by a Pub/Sub message."""
    logging.info(f"Function triggered by Pub/Sub event: {data}")

    try:
        # Decode the Pub/Sub message
        message = base64.b64decode(data['data']).decode('utf-8')
        logging.info(f"Received message data: {message}")

        # Run the DBT command
        dbt_run()

        logging.info("DBT command executed successfully.")
    except Exception as e:
        logging.error(f"Error executing DBT command: {e}")

