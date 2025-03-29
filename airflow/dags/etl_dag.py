"""
ETL Pipeline with Data Quality Validation
This DAG orchestrates an ETL workflow using AWS Glue jobs and validates the output 
data using Great Expectations.
"""

from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import PythonOperator
from great_expectations_provider.operators.validate_checkpoint import GXValidateCheckpointOperator
from airflow import DAG
import os
import sys
import logging

# Add the path for custom modules if needed
sys.path.append('/opt/airflow')

# Path to the Great Expectations context directory
GX_CONTEXT_ROOT_DIR = "/opt/airflow/include/great_expectations"

# Setup Great Expectations on DAG startup (if needed)
def setup_great_expectations():
    """Set up Great Expectations if not already configured"""
    try:
        # Check if the directory exists
        if not os.path.exists(GX_CONTEXT_ROOT_DIR):
            logging.info("Setting up Great Expectations...")
            # Import and run the setup script
            from setup_great_expectations import main as setup_main
            setup_main()
        else:
            logging.info("Great Expectations already set up. Skipping setup.")
    except Exception as e:
        logging.error(f"Error setting up Great Expectations: {e}")
        raise

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
SOURCE_BUCKET = f"nexabrand-{ENV}-source"
TARGET_BUCKET = f"nexabrand-{ENV}-target"

# Create DAG
dag = DAG(
    'etl_pipeline_with_data_quality',
    default_args=default_args,
    description='ETL pipeline with data quality checks for TopDevs data processing',
    schedule_interval='0 0 * * *',  # Daily at midnight
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'glue', 'topdevs', 'great_expectations']
)

# Setup Great Expectations before running any tasks
setup_ge = PythonOperator(
    task_id='setup_great_expectations',
    python_callable=setup_great_expectations,
    dag=dag
)

# Start and End operators
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# Define job configurations with validation settings
job_configs = {
    'products': {
        'dependencies': ['start'],
        'validate': True,
        'checkpoint_name': 'products_checkpoint'
    },
    'customers': {
        'dependencies': ['start'],
        'validate': True,
        'checkpoint_name': 'customers_checkpoint'
    },
    'customer_targets': {
        'dependencies': ['customers'],
        'validate': True,
        'checkpoint_name': 'customer_targets_checkpoint'
    },
    'dates': {
        'dependencies': ['start'],
        'validate': True,
        'checkpoint_name': 'dates_checkpoint'
    },
    'orders': {
        'dependencies': ['products', 'customers', 'dates'],
        'validate': True,
        'checkpoint_name': 'orders_checkpoint'
    },
    'order_lines': {
        'dependencies': ['orders'],
        'validate': True,
        'checkpoint_name': 'order_lines_checkpoint'
    },
    'order_fulfillment': {
        'dependencies': ['order_lines'],
        'validate': True,
        'checkpoint_name': 'order_fulfillment_checkpoint'
    }
}

# Create Glue job tasks and corresponding validation tasks
glue_tasks = {}
validate_tasks = {}

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
    
    # Add validation task for the job
    if config.get('validate', False):
        validate_task_id = f"validate_{job_name}"
        checkpoint_name = config.get('checkpoint_name')
        
        validate_tasks[job_name] = GXValidateCheckpointOperator(
            task_id=validate_task_id,
            data_context_root_dir=GX_CONTEXT_ROOT_DIR,
            checkpoint_name=checkpoint_name,
            fail_task_on_validation_failure=True,
            return_json_dict=True,
            context_type="file",  # Using file-based Data Context
            dag=dag
        )

# Set up task dependencies
setup_ge >> start

for job_name, config in job_configs.items():
    # Set dependencies for Glue jobs
    for dep in config['dependencies']:
        if dep == 'start':
            start >> glue_tasks[job_name]
        else:
            glue_tasks[dep] >> glue_tasks[job_name]
    
    # Connect validation tasks
    if config.get('validate', False):
        glue_tasks[job_name] >> validate_tasks[job_name]
        
        # Check if this is a terminal job
        is_terminal = not any(job_name in c['dependencies'] for c in job_configs.values())
        if is_terminal:
            validate_tasks[job_name] >> end
    else:
        # If no validation, connect directly to end if terminal
        is_terminal = not any(job_name in c['dependencies'] for c in job_configs.values())
        if is_terminal:
            glue_tasks[job_name] >> end