from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow import DAG
from great_expectations_provider.operators.validate_checkpoint import GXValidateCheckpointOperator

# Path to the Great Expectations context directory
GX_CONTEXT_ROOT_DIR = "/opt/airflow/include/great_expectations"

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

# Start and End operators
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# Define job configurations
job_configs = {
    'products': {
        'dependencies': ['start'],
        'validate': True,
        'checkpoint_name': 'products_checkpoint',
        'suite_name': 'products_data_expectation_suite'
    },
    'customers': {
        'dependencies': ['start'],
        'validate': True,
        'checkpoint_name': 'customers_checkpoint',
        'suite_name': 'customers_data_expectation_suite'
    },
    'customer_targets': {
        'dependencies': ['customers'],
        'validate': True,
        'checkpoint_name': 'customer_targets_checkpoint',
        'suite_name': 'customer_targets_data_expectation_suite'
    },
    'dates': {
        'dependencies': ['start'],
        'validate': False
    },
    'orders': {
        'dependencies': ['products', 'customers', 'dates'],
        'validate': True,
        'checkpoint_name': 'orders_checkpoint',
        'suite_name': 'orders_data_expectation_suite'
    },
    'order_lines': {
        'dependencies': ['orders'],
        'validate': True,
        'checkpoint_name': 'order_lines_checkpoint',
        'suite_name': 'order_lines_data_expectation_suite'
    },
    'order_fulfillment': {
        'dependencies': ['order_lines'],
        'validate': True,
        'checkpoint_name': 'order_fulfillment_checkpoint',
        'suite_name': 'order_fulfillment_data_expectation_suite'
    }
}

# Helper function to create a checkpoint configuration function
def make_checkpoint_function(checkpoint_name):
    def _configure_checkpoint(context):
        # Retrieve and return the checkpoint from the GE context by its name.
        return context.checkpoints.get(checkpoint_name)
    return _configure_checkpoint

# Create Glue job tasks and corresponding validation tasks
glue_tasks = {}
validate_tasks = {}

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
    
    # Add validation task if configured for this job
    if config.get('validate', False):
        validate_task_id = f"validate_{job_name}"
        checkpoint_name = config.get('checkpoint_name')
        
        validate_tasks[job_name] = GXValidateCheckpointOperator(
            task_id=validate_task_id,
            data_context_root_dir=GX_CONTEXT_ROOT_DIR,
            configure_checkpoint=make_checkpoint_function(checkpoint_name),
            batch_parameters={'path': f's3://{TARGET_BUCKET}/{job_name}/', 'datasource': 's3_datasource'},
            fail_task_on_validation_failure=True,
            return_json_dict=True,
            context_type="file",
            dag=dag
        )

# Set up task dependencies
for job_name, config in job_configs.items():
    for dep in config['dependencies']:
        if dep == 'start':
            start >> glue_tasks[job_name]
        else:
            glue_tasks[dep] >> glue_tasks[job_name]
    
    if config.get('validate', False):
        glue_tasks[job_name] >> validate_tasks[job_name]
        # If the job is final (i.e. no other job depends on it), route validation to end
        if not any(job_name in c['dependencies'] for c in job_configs.values()):
            validate_tasks[job_name] >> end
    else:
        if not any(job_name in c['dependencies'] for c in job_configs.values()):
            glue_tasks[job_name] >> end
