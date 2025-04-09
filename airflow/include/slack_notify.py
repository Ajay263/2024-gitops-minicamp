import logging
import requests
from airflow.hooks.base import BaseHook
from airflow.utils.state import State
from datetime import datetime

def convert_datetime(execution_date):
    """
    Converts the execution_date to a readable format.
    
    Args:
        execution_date (datetime): The execution date of the Airflow task.
    
    Returns:
        str: The formatted datetime string.
    """
    if isinstance(execution_date, datetime):
        return execution_date.strftime("%Y-%m-%d %H:%M:%S")
    else:
        return str(execution_date)

def construct_message(context):
    """
    Constructs a Slack message using the context from an Airflow task.
    
    Args:
        context (dict): The context dictionary from an Airflow task.
    
    Returns:
        dict: The constructed Slack message payload.
    """
    task_instance = context.get("task_instance")
    failed_tasks = [
        ti.task_id
        for ti in task_instance.get_dagrun().get_task_instances()
        if ti.state == State.FAILED
    ]
    failed_tasks_list = ", ".join(failed_tasks) if failed_tasks else "None"
    
    task_name = task_instance.task_id    
    execution_date = context.get('execution_date')
    log_url = task_instance.log_url if task_instance.log_url else "No logs available"
    dag_run = context.get('dag_run')
    
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "🚨 FAILURE ALERT: Data Pipeline Execution 🚨"
            }
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": "Dag:"
                },
                {
                    "type": "plain_text",
                    "text": task_instance.dag_id
                },
                {
                    "type": "mrkdwn",
                    "text": "Task Name:"
                },
                {
                    "type": "plain_text",
                    "text": task_name
                },
                {
                    "type": "mrkdwn",
                    "text": "Failed Tasks:"
                },
                {
                    "type": "plain_text",
                    "text": failed_tasks_list
                },
                {
                    "type": "mrkdwn",
                    "text": "Execution Time:"
                },
                {
                    "type": "plain_text",
                    "text": convert_datetime(execution_date)
                }
            ]
        },
        {
            "type": "divider"
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"<{log_url}|View Detailed Logs>"
            }
        }
    ]
    
    msg = {
        "blocks": blocks
    }
    
    return msg

def send_failure_alert(context: dict):
    """
    Sends a job failure alert to Slack using the provided context from an Airflow task.
    
    Args:
        context (dict): The context dictionary from an Airflow task.
    """
    try:
        # Get the connection details
        conn = BaseHook.get_connection('slack_webhook_default')
        webhook_endpoint = conn.extra_dejson.get('webhook_endpoint')
        webhook_url = f"{conn.schema}://{conn.host}{webhook_endpoint}"
        
        slack_msg = construct_message(context)
        response = requests.post(
            webhook_url,
            json=slack_msg,
            headers={'Content-Type': 'application/json'}
        )
        if response.status_code != 200:
            logging.error(f'Request to Slack returned an error {response.status_code}, response: {response.text}')
    except Exception as e:
        logging.error(f"Error while sending alert: {e}")