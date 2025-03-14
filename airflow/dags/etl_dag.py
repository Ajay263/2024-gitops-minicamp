from datetime import (
    datetime,
    timedelta,
)

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

from airflow import DAG

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Environment configuration
ENV = "prod"
SOURCE_BUCKET = f"nexabrands-{ENV}-source"
TARGET_BUCKET = f"nexabrands-{ENV}-target"

# Create DAG
dag = DAG(
    'topdevs_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for TopDevs data processing',
    schedule_interval='0 0 * * *',  # Daily at midnight
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'glue', 'topdevs']
)

# Start operator
start = DummyOperator(task_id='start', dag=dag)

# End operator
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
    'dates': {
        'dependencies': ['start']
    },
    'orders': {
        'dependencies': ['products', 'customers', 'dates']
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
    task_id = f"glue_job_{job_name}"
    glue_job_name = f"topdevs-{ENV}-{job_name}-job"

    # Define script arguments
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
    for dep in config['dependencies']:
        if dep == 'start':
            start >> glue_tasks[job_name]
        else:
            glue_tasks[dep] >> glue_tasks[job_name]

    # If this is a final task (no other tasks depend on it), connect it to end
    if not any(job_name in c['dependencies'] for c in job_configs.values()):
        glue_tasks[job_name] >> end
