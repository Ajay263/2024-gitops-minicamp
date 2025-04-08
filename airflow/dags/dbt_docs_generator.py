import os
import shutil
from datetime import datetime
from airflow.decorators import dag, task
from cosmos.operators import DbtDocsOperator
from cosmos import ProfileConfig
from cosmos.profiles import RedshiftUserPasswordProfileMapping
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
import psycopg2
import boto3

# Environment setup.
env = 'local'
dbt_path = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin"
os.environ['PATH'] = f"{dbt_path}:{os.environ['PATH']}"

# Profile configuration for Redshift.
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=RedshiftUserPasswordProfileMapping(
        conn_id="redshift_conn",
        profile_args={
            "schema": "nexabrands_external",
            "dbname": "nexabrands_datawarehouse",
        },
    ),
)

def save_docs_locally(project_dir: str, output_dir: str, **kwargs):
    """
    Save dbt docs files locally, including the assets directory
    Added **kwargs to catch additional parameters like context
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Files to copy from target directory
    files_to_copy = [
        "target/index.html",
        "target/manifest.json",
        "target/graph.gpickle",
        "target/catalog.json",
        "target/run_results.json"  # Optional but useful
    ]
    
    # Copy individual files
    for file in files_to_copy:
        src = os.path.join(project_dir, file)
        dst = os.path.join(output_dir, os.path.basename(file))
        if os.path.exists(src):  # Check if file exists before copying
            shutil.copy2(src, dst)
            print(f"Copied {src} to {dst}")
        else:
            print(f"Warning: {src} not found, skipping.")
    
    # Copy assets directory if it exists
    assets_src = os.path.join(project_dir, "target/assets")  # Changed from "assets" to "target/assets"
    assets_dst = os.path.join(output_dir, "assets")
    
    if os.path.exists(assets_src):
        # Remove existing assets directory if it exists
        if os.path.exists(assets_dst):
            shutil.rmtree(assets_dst)
        
        # Copy the entire assets directory
        shutil.copytree(assets_src, assets_dst)
        print(f"Copied assets directory from {assets_src} to {assets_dst}")
    else:
        print(f"Warning: Assets directory {assets_src} not found, skipping.")

@task
def verify_redshift_connection():
    """Verify Redshift connection before running dbt operations"""
    conn = BaseHook.get_connection("redshift_conn")
    print(f"Connecting to Redshift at {conn.host}:{conn.port} with user {conn.login}")
    try:
        psycopg2.connect(
            dbname=conn.schema,
            user=conn.login,
            password=conn.password,
            host=conn.host,
            port=conn.port
        )
        print("Successfully connected to Redshift!")
    except Exception as e:
        raise Exception(f"Failed to connect to Redshift: {str(e)}")


@task
def upload_docs_to_s3():
    local_dir = "/opt/airflow/dbt-docs/"
    s3_bucket = "nexabrand-prod-target"
    s3_key_prefix = "dbt-docs/"
    
    # Create boto3 client directly
    s3_client = boto3.client('s3')
    
    if not os.path.exists(local_dir):
        raise FileNotFoundError(f"Directory {local_dir} does not exist")
    
    files_uploaded = 0
    
    def upload_directory(directory, base_dir):
        nonlocal files_uploaded
        
        for item in os.listdir(directory):
            local_path = os.path.join(directory, item)
            
            relative_path = os.path.relpath(local_path, base_dir)
            s3_key = f"{s3_key_prefix}{relative_path}"
            
            if os.path.isdir(local_path):
                upload_directory(local_path, base_dir)
            else:
                print(f"Uploading {local_path} to s3://{s3_bucket}/{s3_key}")
                
                # Using boto3 client directly with SSE-S3 encryption
                s3_client.upload_file(
                    Filename=local_path,
                    Bucket=s3_bucket,
                    Key=s3_key,
                    ExtraArgs={
                        'ServerSideEncryption': 'AES256'  # Use SSE-S3 instead of KMS
                    }
                )
                files_uploaded += 1
    
    upload_directory(local_dir, local_dir)
    
    print(f"Successfully uploaded {files_uploaded} files to s3://{s3_bucket}/{s3_key_prefix}")
    return files_uploaded
    
@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["dbt", "docs", "edr", env],
)
def dbt_and_edr_reports_generator():
    # Verify connection first
    connection_check = verify_redshift_connection()
    
    with TaskGroup(group_id="dbt_setup", tooltip="DBT setup tasks") as setup:
        # Install dbt dependencies with better error handling
        install_deps = BashOperator(
            task_id="install_dbt_deps",
            bash_command=f"""
                set -e
                cd /opt/airflow/dbt/nexabrands_dbt
                {dbt_path}/dbt debug --config-dir
                {dbt_path}/dbt deps
                {dbt_path}/dbt run-operation stage_external_sources --vars '{{"ext_full_refresh": True}}'
                {dbt_path}/dbt run --select elementary
            """,
            env={'PATH': os.environ['PATH']},
        )
    
    # Use DbtDocsOperator with properly formatted callback
    # The callback function needs to accept **kwargs to handle the context parameter
    generate_dbt_docs = DbtDocsOperator(
        task_id="generate_dbt_docs",
        project_dir="/opt/airflow/dbt/nexabrands_dbt",
        profile_config=profile_config,
        callback=lambda project_dir, **kwargs: save_docs_locally(project_dir, "/opt/airflow/dbt-docs", **kwargs),
        env={'PATH': os.environ['PATH']}
    )
    
    # Generate and Send Elementary Data Reliability report directly to S3
    generate_edr_report = BashOperator(
        task_id="generate_and_send_edr_report",
        bash_command=f"""
            set -e
            cd /opt/airflow/dbt/nexabrands_dbt
            
            # Generate a temporary report first for local verification
            {dbt_path}/edr report --project-dir /opt/airflow/dbt/nexabrands_dbt
            
            # Send report directly to S3 using Elementary's built-in command
            {dbt_path}/edr send-report \
                --s3-bucket-name nexabrand-prod-target \
                --bucket-file-path edr-report/index.html \
                --update-bucket-website true
            
            echo "EDR report generated and sent to S3 successfully"
        """,
        env={'PATH': os.environ['PATH']},
    )
    
    # Use the existing upload task for dbt docs
    upload_docs = upload_docs_to_s3()
    
    connection_check >> setup >> generate_dbt_docs >> upload_docs >> generate_edr_report

dbt_and_edr_reports_generator()
