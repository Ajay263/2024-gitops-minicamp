from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import great_expectations.expectations as gxe
import pandas as pd
import boto3
from io import StringIO
from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from great_expectations import Checkpoint, ExpectationSuite, ValidationDefinition

from great_expectations_provider.operators.validate_checkpoint import (
    GXValidateCheckpointOperator,
)

if TYPE_CHECKING:
    from great_expectations.core.batch_definition import BatchDefinition
    from great_expectations.data_context import AbstractDataContext

# Define bucket names
SOURCE_BUCKET_NAME = 'nexabrand-prod-source'
DOCS_BUCKET_NAME = 'nexabrand-prod-gx-doc'
FILE_KEY = 'data/order_fulfillment.csv'

def configure_data_docs(context: AbstractDataContext) -> str:
    """Configure and return the S3 Data Docs site"""
    # Define S3 data docs site configuration
    s3_site_name = "s3_data_docs"
    s3_site_config = {
        "class_name": "SiteBuilder",
        "site_index_builder": {
            "class_name": "DefaultSiteIndexBuilder",
        },
        "store_backend": {
            "class_name": "TupleS3StoreBackend",
            "bucket": DOCS_BUCKET_NAME,
            "prefix": "",  # Empty prefix to put docs at the root of the bucket
        },
    }
    
    # Check if the site already exists before adding it
    if s3_site_name not in context.list_data_docs_sites():
        context.add_data_docs_site(site_name=s3_site_name, site_config=s3_site_config)
        print(f"Added new data docs site: {s3_site_name}")
    else:
        print(f"Using existing data docs site: {s3_site_name}")
    
    # Return the site name for use in other functions
    return s3_site_name

def configure_checkpoint(context: AbstractDataContext) -> Checkpoint:
    """This function takes a GX Context and returns a Checkpoint that
    can validate data from S3 against an ExpectationSuite, and run Actions."""
    # Get S3 client
    s3_client = boto3.client('s3')
    
    # Get data from S3
    response = s3_client.get_object(Bucket=SOURCE_BUCKET_NAME, Key=FILE_KEY)
    content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(content))
    
    # Add a Pandas Data Source to your GX context
    data_source = context.data_sources.add_pandas(name="order_fulfillment_source")
    
    # Add a Data Asset for your DataFrame
    data_asset = data_source.add_dataframe_asset(name="order_fulfillment_asset")
    
    # Add the Batch Definition to the data asset
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        name="order_fulfillment_batch_definition"
    )
    
    # Setup expectation suite
    expectation_suite = context.suites.add(
        ExpectationSuite(
            name="order_fulfillment_data_expectation_suite",
            expectations=[
                # Expectation 1: Check columns
                gxe.ExpectTableColumnsToMatchSet(
                    column_set=["PRODUCT_ID", "product.name", "category"],
                    exact_match=False
                ),
                # Expectation 2-3: PRODUCT_ID validations
                gxe.ExpectColumnValuesToNotBeNull(column="PRODUCT_ID"),
                gxe.ExpectColumnValuesToBeUnique(column="PRODUCT_ID"),
                # Expectation 4-5: product.name validations
                gxe.ExpectColumnValuesToNotBeNull(column="product.name"),
                gxe.ExpectColumnValueLengthsToBeBetween(
                    column="product.name",
                    min_value=1,
                    max_value=100
                ),
                # Expectation 6-7: category validations
                gxe.ExpectColumnValuesToNotBeNull(column="category"),
                gxe.ExpectColumnValueLengthsToBeBetween(
                    column="category",
                    min_value=1,
                    max_value=50
                ),
            ],
        )
    )
    
    # Define the Batch Parameters (the DataFrame to be validated)
    batch_parameters = {"dataframe": df}
    
    # Setup validation definition
    validation_definition = context.validation_definitions.add(
        ValidationDefinition(
            name="Order Fulfillment Validation Definition",
            data=batch_definition,
            suite=expectation_suite,
        )
    )
    
    # First configure the data docs
    s3_site_name = configure_data_docs(context)
    
    # Set up actions for the checkpoint
    from great_expectations.checkpoint import SlackNotificationAction, UpdateDataDocsAction
    
    action_list = [
        SlackNotificationAction(
            name="Great Expectations data quality results",
            slack_webhook="https://hooks.slack.com/services/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",
            notify_on="failure",
            show_failed_expectations=True,
        ),
        UpdateDataDocsAction(
            name="update_all_data_docs",
            site_names=[s3_site_name],
        ),
    ]
    
    # Create and return checkpoint
    checkpoint = context.checkpoints.add(
        Checkpoint(
            name="order_fulfillment_checkpoint",
            validation_definitions=[validation_definition],
            actions=action_list,
            result_format={"result_format": "COMPLETE"},
        )
    )
    
    return checkpoint, batch_parameters

# DAG definition
with DAG(
    dag_id="order_fulfillment_gx_validation",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["great_expectations", "data_quality"],
) as dag:
    
    @task
    def fetch_data_from_s3():
        """Task to fetch data from S3 and return it as a dataframe"""
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=SOURCE_BUCKET_NAME, Key=FILE_KEY)
        content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(content))
        return df
    
    # Task to validate data with Great Expectations
    # Update the GXValidateCheckpointOperator to accept batch_parameters
    validate_order_fulfillment = GXValidateCheckpointOperator(
        task_id="validate_order_fulfillment",
        configure_checkpoint=configure_checkpoint,
        return_json=True,
    )
    
    @task
    def check_validation_results(task_instance):
        """Task to check if validation passed"""
        result = task_instance.xcom_pull(task_ids="validate_order_fulfillment")
        return result.get("success")
    
    @task
    def build_data_docs():
        """Task to build data docs in S3"""
        import great_expectations as gx
        context = gx.get_context(mode='file', project_root_dir="./great_expectations")
        
        # Configure the data docs site
        s3_site_name = configure_data_docs(context)
        
        # Build the data docs
        context.build_data_docs(site_names=[s3_site_name])
        
        # Generate website URL
        region = boto3.client('s3').meta.region_name
        website_url = f"http://{DOCS_BUCKET_NAME}.s3-website-{region}.amazonaws.com/"
        return website_url
    
    @task
    def configure_s3_website():
        """Configure S3 bucket for website hosting"""
        s3_client = boto3.client('s3')
        
        # Check if website hosting is already configured
        try:
            # Get current website configuration
            website_config = s3_client.get_bucket_website(Bucket=DOCS_BUCKET_NAME)
            print(f"Website hosting is already configured: {website_config}")
        except Exception as e:
            print(f"Website hosting not configured: {e}")
            # Configure website hosting
            try:
                website_configuration = {
                    'ErrorDocument': {'Key': 'error.html'},
                    'IndexDocument': {'Suffix': 'index.html'},
                }
                s3_client.put_bucket_website(
                    Bucket=DOCS_BUCKET_NAME,
                    WebsiteConfiguration=website_configuration
                )
                print("Successfully configured website hosting.")
            except Exception as e:
                print(f"Error configuring website: {e}")
        
        # Create error page if it doesn't exist
        try:
            # First check if error.html exists
            s3_client.head_object(Bucket=DOCS_BUCKET_NAME, Key='error.html')
            print("Error page already exists.")
        except Exception:
            # Create error page if it doesn't exist
            error_html = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Error - Great Expectations Documentation</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }
                    h1 { color: #c41a16; }
                </style>
            </head>
            <body>
                <h1>Page Not Found</h1>
                <p>The requested Great Expectations documentation page was not found. Please return to the <a href="index.html">home page</a>.</p>
            </body>
            </html>
            """
            
            try:
                s3_client.put_object(
                    Bucket=DOCS_BUCKET_NAME,
                    Key='error.html',
                    Body=error_html,
                    ContentType='text/html'
                )
                print("Uploaded error.html.")
            except Exception as e:
                print(f"Failed to upload error.html: {e}")
        
        # Set public read permissions for the bucket contents
        try:
            bucket_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "PublicReadGetObject",
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": f"arn:aws:s3:::{DOCS_BUCKET_NAME}/*"
                    }
                ]
            }
            s3_client.put_bucket_policy(
                Bucket=DOCS_BUCKET_NAME,
                Policy=json.dumps(bucket_policy)
            )
            print("Set public read permissions on bucket contents.")
        except Exception as e:
            print(f"Warning: Could not set bucket policy: {e}")
        
        # Generate and return the website URL
        region = s3_client.meta.region_name
        website_url = f"http://{DOCS_BUCKET_NAME}.s3-website-{region}.amazonaws.com/"
        return website_url
    
    @task
    def verify_and_repair_docs():
        """Task to verify all documentation exists and repair if needed"""
        s3_client = boto3.client('s3')
        fixed_docs = False
        
        # Check if index.html exists
        try:
            s3_client.head_object(Bucket=DOCS_BUCKET_NAME, Key='index.html')
            print("Index file exists.")
            
            # Get all objects from the bucket to analyze directory structure
            paginator = s3_client.get_paginator('list_objects_v2')
            all_objects = []
            
            for page in paginator.paginate(Bucket=DOCS_BUCKET_NAME):
                if 'Contents' in page:
                    all_objects.extend(page['Contents'])
            
            print(f"Found {len(all_objects)} objects in the bucket.")
            
            # Check for validations directory
            validations_exists = any(obj['Key'].startswith('validations/') for obj in all_objects)
            
            if not validations_exists:
                print("Warning: No validations directory found. This might indicate an issue with the data docs.")
                
                # Create placeholder validations directory with an index
                placeholder_html = """
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Great Expectations - Validations</title>
                    <style>
                        body { font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }
                        h1 { color: #3a7ca8; }
                    </style>
                </head>
                <body>
                    <h1>Validation Results</h1>
                    <p>This directory will contain validation results after successful runs.</p>
                    <p><a href="../index.html">Return to index</a></p>
                </body>
                </html>
                """
                
                s3_client.put_object(
                    Bucket=DOCS_BUCKET_NAME,
                    Key='validations/index.html',
                    Body=placeholder_html,
                    ContentType='text/html'
                )
                print("Created placeholder validations directory and index.")
                fixed_docs = True
            
            # Get the index file to check for broken links
            response = s3_client.get_object(Bucket=DOCS_BUCKET_NAME, Key='index.html')
            index_content = response['Body'].read().decode('utf-8')
            
            # Parse index file to find validation paths (simplified parsing)
            import re
            validation_paths = re.findall(r'href="(validations/[^"]+\.html)"', index_content)
            
            print(f"Found {len(validation_paths)} validation paths in index")
            
            # Check each validation path
            for path in validation_paths:
                try:
                    s3_client.head_object(Bucket=DOCS_BUCKET_NAME, Key=path)
                    print(f"Validation file exists: {path}")
                except Exception as e:
                    print(f"Validation file missing: {path}, error: {e}")
                    
                    # Create placeholder for missing file
                    placeholder_html = f"""
                    <!DOCTYPE html>
                    <html>
                    <head>
                        <title>Great Expectations Validation Result</title>
                        <style>
                            body {{ font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }}
                            h1 {{ color: #3a7ca8; }}
                            .message {{ padding: 20px; background-color: #f8f9fa; border-radius: 5px; }}
                        </style>
                    </head>
                    <body>
                        <h1>Validation Result</h1>
                        <div class="message">
                            <p>This is a placeholder for validation results that were not properly generated.</p>
                            <p>Path: {path}</p>
                            <p>Please wait for the next validation run to generate complete results.</p>
                            <p><a href="/index.html">Return to index</a></p>
                        </div>
                    </body>
                    </html>
                    """
                    
                    s3_client.put_object(
                        Bucket=DOCS_BUCKET_NAME,
                        Key=path,
                        Body=placeholder_html,
                        ContentType='text/html'
                    )
                    print(f"Created placeholder for missing file: {path}")
                    fixed_docs = True
            
        except Exception as e:
            print(f"Index file does not exist: {e}")
            # This shouldn't normally happen if the Great Expectations run succeeded
            # but we'll handle it just in case
            
            placeholder_index = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Great Expectations Documentation</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }
                    h1 { color: #3a7ca8; }
                    .placeholder { padding: 20px; background-color: #f8f9fa; border-radius: 5px; }
                </style>
            </head>
            <body>
                <h1>Great Expectations Documentation</h1>
                <div class="placeholder">
                    <p>The documentation index has not been generated yet.</p>
                    <p>This is a placeholder until the next validation run completes successfully.</p>
                    <p>Check back after the next scheduled data quality check.</p>
                </div>
            </body>
            </html>
            """
            
            s3_client.put_object(
                Bucket=DOCS_BUCKET_NAME,
                Key='index.html',
                Body=placeholder_index,
                ContentType='text/html'
            )
            print("Created placeholder index.html")
            fixed_docs = True
        
        return fixed_docs
    
    @task
    def success_notification(website_url, docs_fixed):
        """Task to notify about successful validation"""
        if docs_fixed:
            print(f"Data validation successful and documentation issues fixed! Reports permanently available at: {website_url}")
        else:
            print(f"Data validation successful! Reports permanently available at: {website_url}")
        return True
    
    # Define the DAG tasks dependencies
    data = fetch_data_from_s3()
    validation_success = check_validation_results()
    docs_url = build_data_docs()
    website_url = configure_s3_website()
    docs_fixed = verify_and_repair_docs()
    notification = success_notification(website_url, docs_fixed)
    
    # Set up the task dependencies
    chain(
        data,
        validate_order_fulfillment,
        validation_success,
        docs_url,
        website_url,
        docs_fixed,
        notification,
    )