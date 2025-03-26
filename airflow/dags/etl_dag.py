from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow import DAG
import sys
import os

# (Optional) Add /opt/airflow for DAG context if needed
sys.path.append('/opt/airflow')

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
    'etl_dag',
    default_args=default_args,
    description='ETL pipeline for TopDevs data processing',
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
    'products': {'dependencies': ['start']},
    'customers': {'dependencies': ['start']},
    'customer_targets': {'dependencies': ['customers']},
    'dates': {'dependencies': ['start']},
    'orders': {'dependencies': ['products', 'customers', 'dates']},
    'order_lines': {'dependencies': ['orders']},
    'order_fulfillment': {'dependencies': ['order_lines']}
}

# Create Glue job tasks
glue_tasks = {}

for job_name, config in job_configs.items():
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
    for dep in config['dependencies']:
        if dep == 'start':
            start >> glue_tasks[job_name]
        else:
            glue_tasks[dep] >> glue_tasks[job_name]
    
    # If the job is final (i.e., no other job depends on it), connect it to the end task
    if not any(job_name in c['dependencies'] for c in job_configs.values()):
        glue_tasks[job_name] >> end
        
        


@task.external_python(python='/opt/airflow/soda_venv/bin/python')
def check_load(scan_name='check_load', checks_subpath='sources'):
    import sys
    import os
    
    # Add necessary paths to ensure module can be found
    sys.path.append('/opt/airflow')
    sys.path.append('/opt/airflow/include')
    
    # Import the check function using the full path
    from include.soda.check_function import check
    
    return check(scan_name, checks_subpath)

# Run Soda checks after the 'orders' Glue job
soda_check = check_load()

glue_tasks['orders'] >> soda_check >> end
