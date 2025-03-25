import great_expectations as gx
import pandas as pd
import boto3
import json
from io import StringIO
from great_expectations.checkpoint import SlackNotificationAction, UpdateDataDocsAction
from great_expectations import Checkpoint, ExpectationSuite, ValidationDefinition

# Set up GX context in file mode (make sure this directory is accessible by your Airflow workers)
context = gx.get_context(mode='file', project_root_dir="./great_expectations")

# Use boto3 to download the data from S3
s3_client = boto3.client('s3')
source_bucket_name = 'nexabrands-prod-source'
docs_bucket_name = 'nexabrands-prod-gx-docs'  # Your new dedicated GX docs bucket
file_key = 'data/order_fulfillment.csv'

response = s3_client.get_object(Bucket=source_bucket_name, Key=file_key)
content = response['Body'].read().decode('utf-8')
order_fulfillment_df = pd.read_csv(StringIO(content))

# Add a Pandas Data Source to your GX context
data_source = context.data_sources.add_pandas(name="order_fulfillment")

# Add a Data Asset for your DataFrame
data_asset = data_source.add_dataframe_asset(name="order_fulfillment")

# Define the Batch Definition name
batch_definition_name = "order_fulfillment_batch"

# Add the Batch Definition to the data asset
batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)
assert batch_definition.name == batch_definition_name

# Create a new Expectation Suite
suite = context.suites.add(gx.ExpectationSuite(name="order_fulfillment_data_expectation_suite"))

# Expectation 1: Check that the table has a defined set of columns (even if extra columns are allowed)
expectation1 = gx.expectations.ExpectTableColumnsToMatchSet(
    column_set=["PRODUCT_ID", "product.name", "category"],
    exact_match=False
)

# Expectation 2: Ensure that the PRODUCT_ID column does not contain null values and is unique
expectation2 = gx.expectations.ExpectColumnValuesToNotBeNull(column="PRODUCT_ID")
expectation3 = gx.expectations.ExpectColumnValuesToBeUnique(column="PRODUCT_ID")

# Expectation 3: Ensure that the product.name column does not contain null values
expectation4 = gx.expectations.ExpectColumnValuesToNotBeNull(column="product.name")

# Expectation 4: Ensure that the product.name column values have a minimum length of 1 and a maximum length of 100
expectation5 = gx.expectations.ExpectColumnValueLengthsToBeBetween(
    column="product.name",
    min_value=1,
    max_value=100
)

# Expectation 5: Ensure that the category column does not contain null values
expectation6 = gx.expectations.ExpectColumnValuesToNotBeNull(column="category")

# Expectation 6: Ensure that the category column values have a minimum length of 1 and a maximum length of 50
expectation7 = gx.expectations.ExpectColumnValueLengthsToBeBetween(
    column="category",
    min_value=1,
    max_value=50
)

# Save the suite
suite.save()

# Define the Batch Parameters (the DataFrame to be validated)
batch_parameters = {"dataframe": order_fulfillment_df}

# Retrieve the batch
batch = batch_definition.get_batch(batch_parameters=batch_parameters)

# Create a Validation Definition
validation_definition = gx.ValidationDefinition(
    data=batch_definition,
    suite=suite,
    name='order_fulfillment'
)

# Add the Validation Definition to the context
validation_definition = context.validation_definitions.add(validation_definition)

# Configure S3 Data Docs with the new dedicated bucket
s3_site_config = {
    "class_name": "SiteBuilder",
    "site_index_builder": {
        "class_name": "DefaultSiteIndexBuilder",
    },
    "store_backend": {
        "class_name": "TupleS3StoreBackend",
        "bucket": docs_bucket_name,  # Using the dedicated GX docs bucket
        "prefix": "",  # Empty prefix to put docs at the root of the bucket
    },
}

# Check if the S3 data docs site already exists before adding it
s3_site_name = "s3_data_docs"
if s3_site_name not in context.list_data_docs_sites():
    context.add_data_docs_site(site_name=s3_site_name, site_config=s3_site_config)
else:
    print(f"Using existing data docs site: {s3_site_name}")

# Set up actions for the checkpoint including Data Docs update and Slack notification
action_list = [
    SlackNotificationAction(
        name="Great Expectations data quality results",
        slack_webhook="https://hooks.slack.com/services/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",
        notify_on="failure",  # Options: "all", "failure", "success"
        show_failed_expectations=True,
    ),
    UpdateDataDocsAction(
        name="update_all_data_docs",
        site_names=[s3_site_name],  # Specify the S3 site to update
    ),
]

# Create checkpoint with the validation definition and actions
checkpoint = gx.Checkpoint(
    name="order_fulfillment_checkpoint",
    validation_definitions=[validation_definition],
    actions=action_list,
    result_format={"result_format": "COMPLETE"},
)

# Add the checkpoint to your context and run it
context.checkpoints.add(checkpoint)
validation_results = checkpoint.run(batch_parameters=batch_parameters)

# Build the data docs manually - this will upload the docs to S3
context.build_data_docs(site_names=[s3_site_name])

# Verify that website hosting is enabled and output the URL
try:
    website_config = s3_client.get_bucket_website(Bucket=docs_bucket_name)
    print(f"Website hosting is configured with index document: {website_config.get('IndexDocument', {}).get('Suffix')}")
    
    region = s3_client.meta.region_name
    website_url = f"http://{docs_bucket_name}.s3-website-{region}.amazonaws.com/"
    print(f"Your Great Expectations Data Docs are available at: {website_url}")
except Exception as e:
    print(f"Warning: {e}")
    print("Attempting to configure website hosting...")
    try:
        website_configuration = {
            'ErrorDocument': {'Key': 'error.html'},
            'IndexDocument': {'Suffix': 'index.html'},
        }
        s3_client.put_bucket_website(
            Bucket=docs_bucket_name,
            WebsiteConfiguration=website_configuration
        )
        print("Website hosting successfully configured.")
        region = s3_client.meta.region_name
        website_url = f"http://{docs_bucket_name}.s3-website-{region}.amazonaws.com/"
        print(f"Your Great Expectations Data Docs are available at: {website_url}")
    except Exception as config_error:
        print(f"Error configuring website hosting: {config_error}")
        print("Your bucket may already be configured via Terraform, but there might be permission issues.")
        print(f"Expected URL: http://{docs_bucket_name}.s3-website-{s3_client.meta.region_name}.amazonaws.com/")

# Create a simple error page if it doesn't exist
try:
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
    
    s3_client.put_object(
        Bucket=docs_bucket_name,
        Key='error.html',
        Body=error_html,
        ContentType='text/html'
    )
    print("Created error.html page")
except Exception as error_page_error:
    print(f"Could not create error page: {error_page_error}")
