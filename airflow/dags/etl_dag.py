import logging
import sys
from datetime import (
    datetime,
    timedelta,
)

import slack_notify
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

from airflow import DAG

# Add the path for custom modules if needed
sys.path.append('/opt/airflow')

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    "on_failure_callback": slack_notify.send_failure_alert,
}

# Environment configuration
ENV = "prod"
SOURCE_BUCKET = f"nexabrand-{ENV}-source"
TARGET_BUCKET = f"nexabrand-{ENV}-target"

# Create DAG
dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for TopDevs data processing',
    start_date=datetime(2025, 4, 10),  # Set start_date directly on the DAG
    schedule_interval='0 0 * * *',  # Daily at midnight
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'glue', 'topdevs']
)

# Start and End operators
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# Define job configurations
job_configs = {
    'products': {
        'dependencies': ['start']
    },
    'customers': {
        'dependencies': ['start']
    },
    'customer_targets': {
        'dependencies': ['customers']
    },
    'orders': {
        'dependencies': ['products', 'customers']
    },
    'order_lines': {
        'dependencies': ['orders']
    },
    'order_fulfillment': {
        'dependencies': ['order_lines']
    }
}

# Create Glue job tasks
glue_tasks = {}
for job_name, config in job_configs.items():
    # Create Glue task
    task_id = f"glue_job_{job_name}"
    glue_job_name = f"topdevs-{ENV}-{job_name}-job"
    
    # Define script arguments for the Glue job
    script_args = {
        "--source-path": f"s3://{SOURCE_BUCKET}/",
        "--destination-path": f"s3://{TARGET_BUCKET}/",
        "--job-name": glue_job_name,
        "--enable-auto-scaling": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--enable-metrics": "true",
        "--environment": ENV
    }
    
    glue_tasks[job_name] = GlueJobOperator(
        task_id=task_id,
        job_name=glue_job_name,
        script_args=script_args,
        region_name='us-east-1',
        wait_for_completion=True,
        num_of_dpus=2,
        dag=dag
    )

# Set up task dependencies
for job_name, config in job_configs.items():
    # Set dependencies for Glue jobs
    for dep in config['dependencies']:
        if dep == 'start':
            start >> glue_tasks[job_name]
        else:
            glue_tasks[dep] >> glue_tasks[job_name]
    
    # Check if this is a terminal job
    is_terminal = not any(job_name in c['dependencies'] for c in job_configs.values())
    if is_terminal:
        glue_tasks[job_name] >> end
