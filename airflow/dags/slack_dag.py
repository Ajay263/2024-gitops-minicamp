from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import slack_notify  # Update this import path if needed

def say_hello():
    print("This dag is going to fail.")
    print(1 / 0)

dag = DAG(
    'hello_world_dag',
    schedule_interval=None,
    default_args={
        "owner": "abc@gmail.com", 
        "start_date": datetime(2023, 6, 27),
        "on_failure_callback": slack_notify.send_failure_alert  # Set at the task level
    }
)

with dag:
    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=say_hello
    )
