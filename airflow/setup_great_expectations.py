#!/usr/bin/env python
"""
This script sets up Great Expectations for your Airflow DAG.
It creates the necessary directory structure, configures the S3 datasource,
creates expectation suites, and sets up checkpoints for validation.
"""
import os
import sys
import boto3
import pandas as pd
import json
import great_expectations as ge
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults
)

# Define constants
GE_DIR = "/opt/airflow/include/great_expectations"
ENV = "prod"
SOURCE_BUCKET = f"nexabrand-{ENV}-source"
TARGET_BUCKET = f"nexabrand-{ENV}-target"

def create_directory_structure():
    """Create the necessary directory structure for Great Expectations"""
    print("Creating Great Expectations directory structure...")
    os.makedirs(f"{GE_DIR}/expectations", exist_ok=True)
    os.makedirs(f"{GE_DIR}/checkpoints", exist_ok=True)
    os.makedirs(f"{GE_DIR}/uncommitted", exist_ok=True)
    os.makedirs(f"{GE_DIR}/plugins", exist_ok=True)
    print("Directory structure created.")

def initialize_data_context():
    """Initialize the Great Expectations data context"""
    print("Initializing Great Expectations data context...")
    
    # Check if great_expectations.yml already exists
    if os.path.exists(f"{GE_DIR}/great_expectations.yml"):
        context = ge.data_context.DataContext(context_root_dir=GE_DIR)
        print("Data context already initialized.")
        return context
    
    # Create a simple DataContext configuration
    data_context_config = DataContextConfig(
        store_backend_defaults=FilesystemStoreBackendDefaults(root_directory=GE_DIR),
        checkpoint_store_name="checkpoint_store",
        expectations_store_name="expectations_store",
        validations_store_name="validations_store",
        evaluation_parameter_store_name="evaluation_parameter_store",
        plugins_directory=f"{GE_DIR}/plugins",
        validation_operators={
            "action_list_operator": {
                "class_name": "ActionListValidationOperator",
                "action_list": [
                    {
                        "name": "store_validation_result",
                        "action": {"class_name": "StoreValidationResultAction"}
                    },
                    {
                        "name": "store_evaluation_params",
                        "action": {"class_name": "StoreEvaluationParametersAction"}
                    },
                    {
                        "name": "update_data_docs",
                        "action": {"class_name": "UpdateDataDocsAction"}
                    }
                ]
            }
        },
        data_docs_sites={
            "local_site": {
                "class_name": "SiteBuilder",
                "show_how_to_buttons": True,
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": f"{GE_DIR}/uncommitted/data_docs/local_site/"
                }
            }
        }
    )
    
    # Create the context
    context = BaseDataContext(project_config=data_context_config)
    context.build_data_docs()
    print("Data context initialized.")
    return context

def configure_s3_datasource(context):
    """Configure the S3 datasource for Great Expectations"""
    print("Configuring S3 datasource...")
    
    # Check if datasource already exists
    if "s3_datasource" in context.list_datasources():
        print("S3 datasource already configured.")
        return
    
    # Configure S3 datasource
    s3_datasource_config = {
        "name": "s3_datasource",
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine"
        },
        "data_connectors": {
            "default_runtime_data_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"],
            },
            "s3_data_connector": {
                "class_name": "ConfiguredAssetS3DataConnector",
                "bucket": TARGET_BUCKET,
                "prefix": "",
                "default_regex": {
                    "pattern": "(.*)\\.csv",
                    "group_names": ["data_asset_name"]
                },
                "assets": {
                    "products": {
                        "pattern": "products/(.*)\\.csv$",
                        "group_names": ["partition"]
                    },
                    "customers": {
                        "pattern": "customers/(.*)\\.csv$",
                        "group_names": ["partition"]
                    },
                    "customer_targets": {
                        "pattern": "customer_targets/(.*)\\.csv$",
                        "group_names": ["partition"]
                    },
                    "dates": {
                        "pattern": "dates/(.*)\\.csv$",
                        "group_names": ["partition"]
                    },
                    "orders": {
                        "pattern": "orders/(.*)\\.csv$",
                        "group_names": ["partition"]
                    },
                    "order_lines": {
                        "pattern": "order_lines/(.*)\\.csv$",
                        "group_names": ["partition"]
                    },
                    "order_fulfillment": {
                        "pattern": "order_fulfillment/(.*)\\.csv$",
                        "group_names": ["partition"]
                    }
                }
            }
        }
    }
    
    # Add the datasource to the DataContext
    context.add_datasource(**s3_datasource_config)
    print("S3 datasource configured.")

def create_expectation_suites(context):
    """Create expectation suites for each data file using the existing JSON configurations"""
    print("Creating expectation suites...")
    
    # Define the mappings between your files and suite names
    suite_mappings = {
        "products": "products_data_expectation_suite",
        "customers": "customers_data_expectation_suite",
        "customer_targets": "customer_targets_data_expectation_suite",
        "orders": "orders_data_expectation_suite",
        "order_lines": "order_lines_data_expectation_suite",
        "order_fulfillment": "order_fulfillment_data_expectation_suite"
    }
    
    # Pre-defined expectations from the JSON files
    suite_expectations = {
        "products_data_expectation_suite": [
            {"type": "expect_table_columns_to_match_set", "kwargs": {"column_set": ["PRODUCT_ID", "product.name", "category"], "exact_match": False}},
            {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "PRODUCT_ID"}},
            {"type": "expect_column_values_to_be_unique", "kwargs": {"column": "PRODUCT_ID"}},
            {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "product.name"}},
            {"type": "expect_column_value_lengths_to_be_between", "kwargs": {"column": "product.name", "min_value": 1, "max_value": 100}},
            {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "category"}},
            {"type": "expect_column_value_lengths_to_be_between", "kwargs": {"column": "category", "min_value": 1, "max_value": 50}}
        ],
        "customers_data_expectation_suite": [
            {"type": "expect_table_columns_to_match_set", "kwargs": {"column_set": ["CUSTOMER_ID", "customer.name", "city"], "exact_match": False}},
            {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "CUSTOMER_ID"}},
            {"type": "expect_column_values_to_be_unique", "kwargs": {"column": "CUSTOMER_ID"}},
            {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "customer.name"}},
            {"type": "expect_column_value_lengths_to_be_between", "kwargs": {"column": "customer.name", "min_value": 1, "max_value": 100}},
            {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "city"}},
            {"type": "expect_column_value_lengths_to_be_between", "kwargs": {"column": "city", "min_value": 1, "max_value": 100}}
        ],
        "customer_targets_data_expectation_suite": [
            {"type": "expect_table_columns_to_match_set", "kwargs": {"column_set": ["CUSTOMER_ID", "ontime_target%", "infull_target%", "OTIF_TARGET%"], "exact_match": False}},
            {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "CUSTOMER_ID"}},
            {"type": "expect_column_values_to_be_unique", "kwargs": {"column": "CUSTOMER_ID"}},
            {"type": "expect_column_values_to_be_between", "kwargs": {"column": "ontime_target%", "min_value": 0.0, "max_value": 100.0, "mostly": 0.95}},
            {"type": "expect_column_values_to_be_between", "kwargs": {"column": "infull_target%", "min_value": 0.0, "max_value": 100.0}},
            {"type": "expect_column_values_to_be_between", "kwargs": {"column": "OTIF_TARGET%", "min_value": 0.0, "max_value": 100.0}}
        ],
        "orders_data_expectation_suite": [
            {"type": "expect_table_columns_to_match_set", "kwargs": {"column_set": ["order_id", "customer_id", "order_placement_date"], "exact_match": False}},
            {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "order_id"}},
            {"type": "expect_column_values_to_be_unique", "kwargs": {"column": "order_id"}},
            {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "customer_id"}},
            {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "order_placement_date"}},
            {"type": "expect_column_values_to_match_strftime_format", "kwargs": {"column": "order_placement_date", "strftime_format": "%A, %B %d"}}
        ],
        "order_lines_data_expectation_suite": [
            {"type": "expect_table_columns_to_match_set", "kwargs": {"column_set": ["order_id", "product_id", "order_qty", "agreed_delivery_date", "actual_delivery_date", "delivery_qty"], "exact_match": False}},
            {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "order_id"}},
            {"type": "expect_column_values_to_be_unique", "kwargs": {"column": "order_id"}},
            {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "product_id"}}
        ],
        "order_fulfillment_data_expectation_suite": [
            {"type": "expect_table_columns_to_match_set", "kwargs": {"column_set": ["ORDER_ID", "on.time", "in_full", "OTIF"], "exact_match": False}},
            {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "ORDER_ID"}},
            {"type": "expect_column_values_to_be_unique", "kwargs": {"column": "ORDER_ID"}},
            {"type": "expect_column_values_to_be_of_type", "kwargs": {"column": "on.time", "type_": "FLOAT"}},
            {"type": "expect_column_values_to_be_between", "kwargs": {"column": "on.time", "min_value": 0.0, "max_value": 1.0}},
            {"type": "expect_column_values_to_be_of_type", "kwargs": {"column": "in_full", "type_": "FLOAT"}},
            {"type": "expect_column_values_to_be_between", "kwargs": {"column": "in_full", "min_value": 0.0, "max_value": 1.0}},
            {"type": "expect_column_values_to_be_of_type", "kwargs": {"column": "OTIF", "type_": "FLOAT"}},
            {"type": "expect_column_values_to_be_between", "kwargs": {"column": "OTIF", "min_value": 0.0, "max_value": 1.0}}
        ]
    }
    
    # Create expectation suites for each data file
    for data_file, suite_name in suite_mappings.items():
        # Skip if suite already exists
        if suite_name in context.list_expectation_suite_names():
            print(f"Expectation suite {suite_name} already exists. Skipping.")
            continue
        
        # Create a new suite
        suite = context.create_expectation_suite(suite_name, overwrite_existing=True)
        
        # Create an empty dataframe as a placeholder
        expectation_df = pd.DataFrame({"dummy": [1, 2, 3]})
        
        # Get a validator
        batch = context.get_batch(
            batch_request=context.build_batch_request(
                datasource_name="s3_datasource",
                data_connector_name="default_runtime_data_connector",
                data_asset_name=data_file,
                runtime_parameters={"batch_data": expectation_df},
                batch_identifiers={"default_identifier_name": "default_identifier"},
            )
        )
        
        validator = batch.validator
        
        # Add expectations from the pre-defined configuration
        if suite_name in suite_expectations:
            for expectation in suite_expectations[suite_name]:
                getattr(validator, expectation["type"])(**expectation["kwargs"])
        
        # Save the suite
        validator.save_expectation_suite(discard_failed_expectations=False)
        print(f"Created expectation suite for {data_file}")
    
    print("All expectation suites created.")

def create_checkpoints(context):
    """Create checkpoints for each data file"""
    print("Creating checkpoints...")
    
    # Define data files
    data_files = [
        "products",
        "customers",
        "customer_targets",
        "dates",
        "orders",
        "order_lines",
        "order_fulfillment"
    ]
    
    # Create checkpoints for each data file
    for data_file in data_files:
        suite_name = f"{data_file}_data_expectation_suite"
        checkpoint_name = f"{data_file}_checkpoint"
        
        # Skip if checkpoint already exists
        if checkpoint_name in context.list_checkpoints():
            print(f"Checkpoint {checkpoint_name} already exists. Skipping.")
            continue
        
        # Configure the checkpoint
        checkpoint_config = {
            "name": checkpoint_name,
            "config_version": 1.0,
            "class_name": "SimpleCheckpoint",
            "expectation_suite_name": suite_name,
            "run_name_template": "%Y%m%d-%H%M%S-" + checkpoint_name,
            "validations": [
                {
                    "batch_request": {
                        "datasource_name": "s3_datasource",
                        "data_connector_name": "s3_data_connector",
                        "data_asset_name": data_file,
                        "data_connector_query": {
                            "index": -1  # Get the latest file
                        }
                    }
                }
            ]
        }
        
        # Add the checkpoint
        context.add_checkpoint(**checkpoint_config)
        print(f"Created checkpoint for {data_file}")
    
    print("All checkpoints created.")

def create_json_expectations():
    """Write the expectation suite JSON files to disk"""
    print("Creating JSON expectation files...")
    
    # Make sure the expectations directory exists
    os.makedirs(f"{GE_DIR}/expectations", exist_ok=True)
    
    # Products expectations
    products_suite = {
        "expectations": [
            {
                "id": "561cfd5d-be01-4a86-b2aa-7011d38b8459",
                "kwargs": {"column_set": ["PRODUCT_ID", "product.name", "category"], "exact_match": False},
                "meta": {},
                "type": "expect_table_columns_to_match_set"
            },
            {
                "id": "ec41fc78-0ad7-4f98-ad56-1af521c42c5a",
                "kwargs": {"column": "PRODUCT_ID"},
                "meta": {},
                "type": "expect_column_values_to_not_be_null"
            },
            {
                "id": "b49fc2f2-1678-4580-bf24-6a57616ba2cd",
                "kwargs": {"column": "PRODUCT_ID"},
                "meta": {},
                "type": "expect_column_values_to_be_unique"
            },
            {
                "id": "9a8ea578-3237-49d6-b236-961f2761f9ff",
                "kwargs": {"column": "product.name"},
                "meta": {},
                "type": "expect_column_values_to_not_be_null"
            },
            {
                "id": "fea5a63b-8d1d-4510-8478-9e6fe7bdf2a3",
                "kwargs": {"column": "product.name", "max_value": 100, "min_value": 1},
                "meta": {},
                "type": "expect_column_value_lengths_to_be_between"
            },
            {
                "id": "d3295187-ac1a-49ed-b427-6fa054c121dc",
                "kwargs": {"column": "category"},
                "meta": {},
                "type": "expect_column_values_to_not_be_null"
            },
            {
                "id": "f1441acb-d49b-4ad7-ada9-5dd611ad042f",
                "kwargs": {"column": "category", "max_value": 50, "min_value": 1},
                "meta": {},
                "type": "expect_column_value_lengths_to_be_between"
            }
        ],
        "id": "208e8726-121a-4975-945b-21089f051b28",
        "meta": {"great_expectations_version": "1.3.10"},
        "name": "products_data_expectation_suite",
        "notes": None
    }
    
    # Customers expectations
    customers_suite = {
        "expectations": [
            {
                "id": "6667fd60-e73b-4091-8358-31186b8312e2",
                "kwargs": {"column_set": ["CUSTOMER_ID", "customer.name", "city"], "exact_match": False},
                "meta": {},
                "type": "expect_table_columns_to_match_set"
            },
            {
                "id": "b2f8616f-8b42-4cb5-83b6-d48fdd840fcd",
                "kwargs": {"column": "CUSTOMER_ID"},
                "meta": {},
                "type": "expect_column_values_to_not_be_null"
            },
            {
                "id": "3f96fc17-2473-4201-a7cf-fa29a252f3f0",
                "kwargs": {"column": "CUSTOMER_ID"},
                "meta": {},
                "type": "expect_column_values_to_be_unique"
            },
            {
                "id": "f6c2386b-1840-4107-9656-b1d531cb2fb8",
                "kwargs": {"column": "customer.name"},
                "meta": {},
                "type": "expect_column_values_to_not_be_null"
            },
            {
                "id": "28c5130c-4b01-42b5-a408-bf043776a0c6",
                "kwargs": {"column": "customer.name", "max_value": 100, "min_value": 1},
                "meta": {},
                "type": "expect_column_value_lengths_to_be_between"
            },
            {
                "id": "58dcc13d-ffde-4646-8553-e4d87e72b1f9",
                "kwargs": {"column": "city"},
                "meta": {},
                "type": "expect_column_values_to_not_be_null"
            },
            {
                "id": "ce3aea79-965f-4183-a223-e2d3db24e6de",
                "kwargs": {"column": "city", "max_value": 100, "min_value": 1},
                "meta": {},
                "type": "expect_column_value_lengths_to_be_between"
            }
        ],
        "id": "df180b81-c3d7-4f8f-9ea4-d0b7bd260056",
        "meta": {"great_expectations_version": "1.3.10"},
        "name": "customers_data_expectation_suite",
        "notes": None
    }
    
    # Customer targets expectations
    customer_targets_suite = {
        "expectations": [
            {
                "id": "cc22bc5a-1603-4cb4-ad09-05df6c4e7378",
                "kwargs": {"column_set": ["CUSTOMER_ID", "ontime_target%", "infull_target%", "OTIF_TARGET%"], "exact_match": False},
                "meta": {},
                "type": "expect_table_columns_to_match_set"
            },
            {
                "id": "a695c9d9-30c3-491a-bc8d-0a47e79d308d",
                "kwargs": {"column": "CUSTOMER_ID"},
                "meta": {},
                "type": "expect_column_values_to_not_be_null"
            },
            {
                "id": "5d715a34-ab98-4e87-a041-05bb4bffa657",
                "kwargs": {"column": "CUSTOMER_ID"},
                "meta": {},
                "type": "expect_column_values_to_be_unique"
            },
            {
                "id": "19621c47-aaef-44df-8ef5-9566eb1a95a5",
                "kwargs": {"column": "ontime_target%", "max_value": 100.0, "min_value": 0.0, "mostly": 0.95},
                "meta": {},
                "type": "expect_column_values_to_be_between"
            },
            {
                "id": "b2a7b47e-0e8f-4fcf-9c30-9d83afc384d3",
                "kwargs": {"column": "infull_target%", "max_value": 100.0, "min_value": 0.0},
                "meta": {},
                "type": "expect_column_values_to_be_between"
            },
            {
                "id": "df3d549c-76ef-42cc-9cf6-c13836c0d76a",
                "kwargs": {"column": "OTIF_TARGET%", "max_value": 100.0, "min_value": 0.0},
                "meta": {},
                "type": "expect_column_values_to_be_between"
            }
        ],
        "id": "0c6d3c3e-2d2d-45d4-a283-03abf1bfb9bd",
        "meta": {"great_expectations_version": "1.3.10"},
        "name": "customer_targets_data_expectation_suite",
        "notes": None
    }
    
    # Orders expectations
    orders_suite = {
        "expectations": [
            {
                "id": "ed20e04b-9280-472c-8bd7-f876d1ff0dc8",
                "kwargs": {"column_set": ["order_id", "customer_id", "order_placement_date"], "exact_match": False},
                "meta": {},
                "type": "expect_table_columns_to_match_set"
            },
            {
                "id": "faef2957-c901-4f5e-be89-3806a36af152",
                "kwargs": {"column": "order_id"},
                "meta": {},
                "type": "expect_column_values_to_not_be_null"
            },
            {
                "id": "b7919bc2-f17c-4d73-b28c-dee31606943a",
                "kwargs": {"column": "order_id"},
                "meta": {},
                "type": "expect_column_values_to_be_unique"
            },
            {
                "id": "fb28bb4f-77d8-4c01-aef1-70a3ea73f303",
                "kwargs": {"column": "customer_id"},
                "meta": {},
                "type": "expect_column_values_to_not_be_null"
            },
            {
                "id": "3e7126ac-6c89-42ea-bdfc-7da9ecd7d5a3",
                "kwargs": {"column": "order_placement_date"},
                "meta": {},
                "type": "expect_column_values_to_not_be_null"
            },
            {
                "id": "359d9f7a-369b-47e4-a516-09883a4ee308",
                "kwargs": {"column": "order_placement_date", "strftime_format": "%A, %B %d"},
                "meta": {},
                "type": "expect_column_values_to_match_strftime_format"
            }
        ],
        "id": "7020f13a-770d-40df-9a73-b01f74c3845b",
        "meta": {"great_expectations_version": "1.3.10"},
        "name": "orders_data_expectation_suite",
        "notes": None
    }
    
    # Order lines expectations
    order_lines_suite = {
        "expectations": [
            {
                "id": "bf1400d5-c1f0-4481-ace2-0d902facc12e",
                "kwargs": {"column_set": ["order_id", "product_id", "order_qty", "agreed_delivery_date", "actual_delivery_date", "delivery_qty"], "exact_match": False},
                "meta": {},
                "type": "expect_table_columns_to_match_set"
            },
            {
                "id": "c360fe0d-589f-4bb0-bfb5-b9ebb5a294ac",
                "kwargs": {"column": "order_id"},
                "meta": {},
                "type": "expect_column_values_to_not_be_null"
            },
            {
                "id": "34789ffb-0cc6-4949-8cdc-3d33163c2830",
                "kwargs": {"column": "order_id"},
                "meta": {},
                "type": "expect_column_values_to_be_unique"
            },
            {
                "id": "bc20bc58-ff8d-446f-b7d1-c088ba51152c",
                "kwargs": {"column": "product_id"},
                "meta": {},
                "type": "expect_column_values_to_not_be_null"
            }
        ],
        "id": "158c1d5d-30c2-473e-8c58-7b2990a7ef44",
        "meta": {"great_expectations_version": "1.3.10"},
        "name": "order_lines_data_expectation_suite",
        "notes": None
    }
    
    # Order fulfillment expectations
    order_fulfillment_suite = {
        "expectations": [
            {
                "id": "87057ab9-e830-4aa3-a7be-40c36e993371",
                "kwargs": {"column_set": ["ORDER_ID", "on.time", "in_full", "OTIF"], "exact_match": False},
                "meta": {},
                "type": "expect_table_columns_to_match_set"
            },
            {
                "id": "bb9e456b-38d2-4165-9c55-16774dd278c1",
                "kwargs": {"column": "ORDER_ID"},
                "meta": {},
                "type": "expect_column_values_to_not_be_null"
            },
            {
                "id": "c9301ae4-3996-4d73-b038-26476bac4e95",
                "kwargs": {"column": "ORDER_ID"},
                "meta": {},
                "type": "expect_column_values_to_be_unique"
            },
            {
                "id": "1318cf61-29cc-40c8-8f94-f716f15b891e",
                "kwargs": {"column": "on.time", "type_": "FLOAT"},
                "meta": {},
                "type": "expect_column_values_to_be_of_type"
            },
            {
                "id": "af90cff6-8c39-4962-9ed3-4296733b80e3",
                "kwargs": {"column": "on.time", "max_value": 1.0, "min_value": 0.0},
                "meta": {},
                "type": "expect_column_values_to_be_between"
            },
            {
                "id": "91d89e9e-956d-46e8-b93a-c4ece139aee0",
                "kwargs": {"column": "in_full", "type_": "FLOAT"},
                "meta": {},
                "type": "expect_column_values_to_be_of_type"
            },
            {
                "id": "418681b6-121c-4289-92eb-5312acd41dc2",
                "kwargs": {"column": "in_full", "max_value": 1.0, "min_value": 0.0},
                "meta": {},
                "type": "expect_column_values_to_be_between"
            },
            {
                "id": "eaf15b08-7531-4804-a343-044dc5d399de",
                "kwargs": {"column": "OTIF", "type_": "FLOAT"},
                "meta": {},
                "type": "expect_column_values_to_be_of_type"
            },
            {
                "id": "a432c325-7620-42f3-9142-aa24074d0308",
                "kwargs": {"column": "OTIF", "max_value": 1.0, "min_value": 0.0},
                "meta": {},
                "type": "expect_column_values
                }
                
     