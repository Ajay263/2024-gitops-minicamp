import os
import shutil
import subprocess
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
    elementary_dest_dir = os.path.join(output_dir, "elementary")
    
    # Create elementary directory if it doesn't exist
    if not os.path.exists(elementary_dest_dir):
        os.makedirs(elementary_dest_dir)
    
    # Check all possible locations for Elementary report
    possible_locations = [
        os.path.join(project_dir, "elementary_report"),  # Directory for multiple files
        os.path.join(project_dir, "edr_report.html"),    # Single HTML file
        os.path.join(project_dir, "target", "elementary_report"),  # Alternative directory
        os.path.join(project_dir, "target", "edr_report.html"),    # Alternative file location
    ]
    
    report_found = False
    
    # First check for directory-based reports
    for location in possible_locations:
        if os.path.exists(location) and os.path.isdir(location):
            print(f"Found Elementary report directory at {location}")
            
            # Copy all files from the report directory
            for item in os.listdir(location):
                src_path = os.path.join(location, item)
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
            
            report_found = True
            break
    
    # If no directory found, check for single file reports
    if not report_found:
        for location in possible_locations:
            if os.path.exists(location) and os.path.isfile(location):
                print(f"Found Elementary report file at {location}")
                
                # Copy the report file as index.html
                dst_path = os.path.join(elementary_dest_dir, "index.html")
                shutil.copy2(location, dst_path)
                print(f"Copied Elementary report from {location} to {dst_path}")
                
                report_found = True
                break
    
    if not report_found:
        print("Warning: Could not find Elementary report in any expected location.")
        # List directory contents for debugging
        print(f"Project directory contents ({project_dir}):")
        for item in os.listdir(project_dir):
            print(f" - {item}")
        
        if os.path.exists(os.path.join(project_dir, "target")):
            print(f"Target directory contents ({os.path.join(project_dir, 'target')}):")
            for item in os.listdir(os.path.join(project_dir, "target")):
                print(f" - {item}")

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

@task(multiple_outputs=True)
def generate_elementary_report():
    """Generate Elementary report using edr command"""
    project_dir = "/opt/airflow/dbt/nexabrands_dbt"
    
    try:
        # Change to project directory
        os.chdir(project_dir)
        
        # Check Elementary installation and version
        print("Checking Elementary installation...")
        try:
            result = subprocess.run([f"{dbt_path}/edr", "--version"], 
                                    capture_output=True, text=True, check=False)
            print(f"Elementary version: {result.stdout.strip()}")
        except Exception as e:
            print(f"Could not get Elementary version: {str(e)}")
        
        # Try to generate dbt artifacts first if they don't exist
        if not os.path.exists(os.path.join(project_dir, "target", "manifest.json")):
            print("Manifest not found. Running dbt compile first...")
            compile_result = subprocess.run(
                [f"{dbt_path}/dbt", "compile"], 
                capture_output=True, 
                text=True, 
                check=False
            )
            print(f"dbt compile stdout: {compile_result.stdout}")
            print(f"dbt compile stderr: {compile_result.stderr}")
        
        # Generate Elementary report using subprocess for better error handling
        print("Generating Elementary report...")
        result = subprocess.run(
            [f"{dbt_path}/edr report"], 
            capture_output=True, 
            text=True, 
            check=False
        )
        
        print(f"edr report stdout: {result.stdout}")
        print(f"edr report stderr: {result.stderr}")
        
        if result.returncode != 0:
            print(f"Elementary report command failed with code {result.returncode}")
            
            # Try alternative command format
            print("Trying alternative command format...")
            alt_result = subprocess.run(
                f"cd {project_dir} && {dbt_path}/edr report --file edr_report.html",
                shell=True,
                capture_output=True,
                text=True,
                check=False
            )
            
            print(f"Alternative command stdout: {alt_result.stdout}")
            print(f"Alternative command stderr: {alt_result.stderr}")
            
            if alt_result.returncode != 0:
                print("Both Elementary report commands failed")
                # Continue execution to check if report was generated despite error
            else:
                print("Alternative command succeeded")
        else:
            print("Elementary report command succeeded")
        
        # Save the report to the specified directory
        save_elementary_report(project_dir, "/opt/airflow/dbt-docs")
        
        return {
            "success": True,
            "report_path": "/opt/airflow/dbt-docs/elementary"
        }
        
    except Exception as e:
        print(f"Error in generate_elementary_report: {str(e)}")
        # Still try to save any report that might have been generated
        try:
            save_elementary_report(project_dir, "/opt/airflow/dbt-docs")
        except Exception as save_error:
            print(f"Error while trying to save report: {str(save_error)}")
        
        return {
            "success": False,
            "error": str(e)
        }

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
                
                # Check if Elementary is installed correctly
                if {dbt_path}/edr --version; then
                    echo "Elementary is installed correctly"
                else
                    echo "Elementary might not be installed correctly, checking..."
                    pip list | grep elementary
                fi
                
                # Run the external sources operation
                {dbt_path}/dbt run-operation stage_external_sources --vars '{{"ext_full_refresh": True}}'
                
                # Make sure dbt artifacts are generated
                {dbt_path}/dbt compile
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
    
    # Generate Elementary report
    elementary_report = generate_elementary_report()
    
    # Use a BashOperator as a fallback approach for generating Elementary report
    elementary_fallback = BashOperator(
        task_id="elementary_fallback",
        bash_command=f"""
            set -e
            cd /opt/airflow/dbt/nexabrands_dbt
            
            # Make sure dbt artifacts exist
            {dbt_path}/dbt compile --no-partial-parse
            
            # Generate Elementary report
            {dbt_path}/edr report --file /opt/airflow/dbt-docs/elementary/index.html
            
            # Check if report was generated
            if [ -f "/opt/airflow/dbt-docs/elementary/index.html" ]; then
                echo "Elementary report generated successfully"
                exit 0
            else
                echo "Elementary report generation failed"
                mkdir -p /opt/airflow/dbt-docs/elementary
                echo "<html><body><h1>Elementary Report Generation Failed</h1><p>Check logs for details.</p></body></html>" > /opt/airflow/dbt-docs/elementary/index.html
                exit 0  # Continue workflow despite error
            fi
        """,
        env={'PATH': os.environ['PATH']},
        trigger_rule='all_done'  # Run this task regardless of upstream task's status
    )
    
    # Upload both dbt docs and Elementary report to S3
    upload_docs = upload_docs_to_s3()
    
    # Define task dependencies
    connection_check >> setup >> generate_dbt_docs >> elementary_report >> elementary_fallback >> upload_docs

dbt_docs_generator()
