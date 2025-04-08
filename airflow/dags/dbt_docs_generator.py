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
    assets_src = os.path.join(project_dir, "target/assets")
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

def save_elementary_report(project_dir: str, output_dir: str, **kwargs):
    """
    Save Elementary report files locally
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Elementary report is stored in this directory
    elementary_report_dir = os.path.join(project_dir, "elementary_report")
    
    if os.path.exists(elementary_report_dir):
        # The destination directory for Elementary report
        elementary_dest_dir = os.path.join(output_dir, "elementary")
        
        # Create elementary directory if it doesn't exist
        if not os.path.exists(elementary_dest_dir):
            os.makedirs(elementary_dest_dir)
        
        # Copy all files from elementary_report directory
        for item in os.listdir(elementary_report_dir):
            src_path = os.path.join(elementary_report_dir, item)
            dst_path = os.path.join(elementary_dest_dir, item)
            
            if os.path.isdir(src_path):
                # If it's a directory, copy it recursively
                if os.path.exists(dst_path):
                    shutil.rmtree(dst_path)
                shutil.copytree(src_path, dst_path)
                print(f"Copied directory {src_path} to {dst_path}")
            else:
                # If it's a file, copy it directly
                shutil.copy2(src_path, dst_path)
                print(f"Copied file {src_path} to {dst_path}")
    else:
        print(f"Warning: Elementary report directory {elementary_report_dir} not found.")
        # Try to find report in alternative location
        edr_report_path = os.path.join(project_dir, "edr_report.html")
        if os.path.exists(edr_report_path):
            elementary_dest_dir = os.path.join(output_dir, "elementary")
            if not os.path.exists(elementary_dest_dir):
                os.makedirs(elementary_dest_dir)
            
            # Copy the edr_report.html file
            dst_path = os.path.join(elementary_dest_dir, "index.html")
            shutil.copy2(edr_report_path, dst_path)
            print(f"Copied Elementary report from {edr_report_path} to {dst_path}")
        else:
            print(f"Warning: Could not find Elementary report at {edr_report_path}")

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

@task
def generate_elementary_report():
    """Generate Elementary report using edr command"""
    project_dir = "/opt/airflow/dbt/nexabrands_dbt"
    
    try:
        # Change to project directory
        os.chdir(project_dir)
        
        # Generate Elementary report using edr command
        command = f"{dbt_path}/edr report"
        
        # Execute the command
        exit_code = os.system(command)
        
        if exit_code != 0:
            raise Exception(f"Elementary report generation failed with exit code {exit_code}")
        
        print("Elementary report generated successfully!")
        
        # Save the report to the specified directory
        save_elementary_report(project_dir, "/opt/airflow/dbt-docs")
        
    except Exception as e:
        raise Exception(f"Error generating Elementary report: {str(e)}")

@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["dbt", "docs", env],
)
def dbt_docs_generator():
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
                # Run the external sources operation
                {dbt_path}/dbt run-operation stage_external_sources --vars '{{"ext_full_refresh": True}}'
            """,
            env={'PATH': os.environ['PATH']},
        )
    
    # Generate dbt docs
    generate_dbt_docs = DbtDocsOperator(
        task_id="generate_dbt_docs",
        project_dir="/opt/airflow/dbt/nexabrands_dbt",
        profile_config=profile_config,
        callback=lambda project_dir, **kwargs: save_docs_locally(project_dir, "/opt/airflow/dbt-docs", **kwargs),
        env={'PATH': os.environ['PATH']}
    )
    
    # Generate Elementary report as a separate task
    elementary_report = generate_elementary_report()
    
    # Upload both dbt docs and Elementary report to S3
    upload_docs = upload_docs_to_s3()
    
    # Define task dependencies
    connection_check >> setup >> generate_dbt_docs >> elementary_report >> upload_docs

dbt_docs_generator()
