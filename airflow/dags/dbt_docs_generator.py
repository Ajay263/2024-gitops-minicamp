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
def upload_to_s3(source_dirs=None):
    """
    Upload multiple directories to S3 with proper prefix handling
    
    Args:
        source_dirs: Dictionary mapping local directories to S3 prefixes
                    e.g. {"/opt/airflow/dbt-docs/": "dbt-docs/", 
                          "/opt/airflow/edr-report/": "edr-report/"}
    """
    if source_dirs is None:
        source_dirs = {
            "/opt/airflow/dbt-docs/": "dbt-docs/",
            "/opt/airflow/edr-report/": "edr-report/"
        }
        
    s3_bucket = "nexabrand-prod-target"
    
    # Create boto3 client directly
    s3_client = boto3.client('s3')
    
    total_files_uploaded = 0
    
    for local_dir, s3_key_prefix in source_dirs.items():
        if not os.path.exists(local_dir):
            print(f"Warning: Directory {local_dir} does not exist, skipping.")
            continue
            
        files_uploaded = 0
        
        def upload_directory(directory, base_dir, prefix):
            nonlocal files_uploaded
            
            for item in os.listdir(directory):
                local_path = os.path.join(directory, item)
                
                relative_path = os.path.relpath(local_path, base_dir)
                s3_key = f"{prefix}{relative_path}"
                
                if os.path.isdir(local_path):
                    upload_directory(local_path, base_dir, prefix)
                else:
                    print(f"Uploading {local_path} to s3://{s3_bucket}/{s3_key}")
                    
                    # Using boto3 client directly with SSE-S3 encryption
                    s3_client.upload_file(
                        Filename=local_path,
                        Bucket=s3_bucket,
                        Key=s3_key,
                        ExtraArgs={
                            'ServerSideEncryption': 'AES256',  # Use SSE-S3 encryption
                            'ContentType': get_content_type(local_path)  # Set proper content type
                        }
                    )
                    files_uploaded += 1
        
        upload_directory(local_dir, local_dir, s3_key_prefix)
        
        print(f"Successfully uploaded {files_uploaded} files to s3://{s3_bucket}/{s3_key_prefix}")
        total_files_uploaded += files_uploaded
    
    # Configure website hosting for the EDR report if needed
    try:
        # Configure the bucket for website hosting
        s3_client.put_bucket_website(
            Bucket=s3_bucket,
            WebsiteConfiguration={
                'IndexDocument': {'Suffix': 'index.html'},
                'ErrorDocument': {'Key': 'error.html'}
            }
        )
        
        # Make the EDR report the index document
        s3_client.copy_object(
            Bucket=s3_bucket,
            CopySource=f"{s3_bucket}/edr-report/index.html",
            Key="index.html",
            MetadataDirective="REPLACE",
            ContentType="text/html",
            ServerSideEncryption="AES256"
        )
        print(f"Successfully configured website hosting for bucket {s3_bucket}")
    except Exception as e:
        print(f"Warning: Failed to configure website hosting: {str(e)}")
        
    return total_files_uploaded

def get_content_type(file_path):
    """Determine content type based on file extension"""
    file_extension = os.path.splitext(file_path)[1].lower()
    
    content_types = {
        '.html': 'text/html',
        '.css': 'text/css',
        '.js': 'application/javascript',
        '.json': 'application/json',
        '.png': 'image/png',
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.gif': 'image/gif',
        '.svg': 'image/svg+xml',
        '.txt': 'text/plain'
    }
    
    return content_types.get(file_extension, 'application/octet-stream')
    
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
    
    # Generate dbt docs
    generate_dbt_docs = DbtDocsOperator(
        task_id="generate_dbt_docs",
        project_dir="/opt/airflow/dbt/nexabrands_dbt",
        profile_config=profile_config,
        callback=lambda project_dir, **kwargs: save_docs_locally(project_dir, "/opt/airflow/dbt-docs", **kwargs),
        env={'PATH': os.environ['PATH']}
    )
    
    # Generate Elementary Data Reliability report
    generate_edr_report = BashOperator(
        task_id="generate_edr_report",
        bash_command=f"""
            set -e
            cd /opt/airflow/dbt/nexabrands_dbt
            
            # Create directory to store the EDR report
            mkdir -p /opt/airflow/edr-report
            
            # Run Elementary to generate the report
            {dbt_path}/edr report \
                --file-path /opt/airflow/edr-report/index.html \
                --profiles-dir /opt/airflow/dbt/nexabrands_dbt/profiles \
                --project-dir /opt/airflow/dbt/nexabrands_dbt
            
            echo "EDR report generated successfully at /opt/airflow/edr-report/index.html"
        """,
        env={'PATH': os.environ['PATH']},
    )
    
    # Upload both dbt docs and EDR report to S3
    upload_reports = upload_to_s3()
    
    connection_check >> setup >> generate_dbt_docs >> generate_edr_report >> upload_reports

dbt_and_edr_reports_generator()
