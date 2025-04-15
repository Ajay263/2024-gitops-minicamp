import os
from unittest.mock import (
    MagicMock,
    patch,
)

import boto3
import psycopg2
import pytest
from airflow.models import DagBag

# Path to your DAGs folder
DAG_PATH = "/opt/airflow/dags"

@pytest.fixture(scope="session")
def dagbag():
    """Fixture that provides a DagBag loaded with your DAGs"""
    return DagBag(dag_folder=DAG_PATH, include_examples=False)

@pytest.fixture
def dag(dagbag):
    """Fixture that provides the dbt_docs_generator DAG"""
    dag_id = "dbt_docs_generator"
    return dagbag.get_dag(dag_id)

@pytest.fixture
def etl_pipeline_dag(dagbag):
    """Fixture that provides the etl_pipeline DAG"""
    dag_id = "etl_pipeline"
    return dagbag.get_dag(dag_id)

@pytest.fixture
def mock_redshift_conn():
    """Mock Redshift connection for testing"""
    with patch('airflow.hooks.base.BaseHook.get_connection') as mock_get_conn:
        conn = MagicMock()
        conn.host = "redshift-test.amazonaws.com"
        conn.port = 5439
        conn.login = "test_user"
        conn.password = "test_password"
        conn.schema = "test_db"
        mock_get_conn.return_value = conn
        yield mock_get_conn

@pytest.fixture
def mock_psycopg2_connect():
    """Mock psycopg2 connection for testing"""
    with patch('psycopg2.connect') as mock_connect:
        mock_connect.return_value = MagicMock()
        yield mock_connect

@pytest.fixture
def mock_s3_client():
    """Mock boto3 S3 client for testing"""
    with patch('boto3.client') as mock_client:
        mock_s3 = MagicMock()
        mock_client.return_value = mock_s3
        yield mock_s3

@pytest.fixture
def mock_os_path_exists():
    """Mock os.path.exists to simulate file existence"""
    with patch('os.path.exists') as mock_exists:
        mock_exists.return_value = True
        yield mock_exists

@pytest.fixture
def mock_os_listdir():
    """Mock os.listdir to return test files with limited recursion"""
    with patch('os.listdir') as mock_listdir:
        # Define a side effect function that handles directories differently
        call_count = [0]  # Use a list to maintain state between calls
        
        def listdir_side_effect(path):
            call_count[0] += 1
            # Base case - first level
            if call_count[0] == 1:
                return ["index.html", "manifest.json", "catalog.json", "assets"]
            # For "assets" directory - return empty list after first level to prevent recursion
            elif "assets" in path:
                return []
            else:
                return ["test.html"]
        
        mock_listdir.side_effect = listdir_side_effect
        yield mock_listdir

@pytest.fixture
def mock_os_path_isdir():
    """Mock os.path.isdir with limited recursion for directory checks"""
    with patch('os.path.isdir') as mock_isdir:
        call_count = [0]  # Use a list to maintain state between calls
        
        def is_dir_side_effect(path):
            call_count[0] += 1
            # Only identify the initial "assets" as a directory, not nested ones
            if path.endswith("assets") and call_count[0] <= 2:
                return True
            return False
        
        mock_isdir.side_effect = is_dir_side_effect
        yield mock_isdir

@pytest.fixture
def mock_dbt_docs_operator():
    """Mock DbtDocsOperator for testing"""
    with patch('cosmos.operators.DbtDocsOperator.execute') as mock_execute:
        mock_execute.return_value = None
        yield mock_execute

@pytest.fixture
def mock_bash_operator():
    """Mock BashOperator for testing"""
    with patch('airflow.operators.bash.BashOperator.execute') as mock_execute:
        mock_execute.return_value = None
        yield mock_execute

@pytest.fixture
def mock_shutil():
    """Mock shutil for testing file operations"""
    with patch('shutil.copy2') as mock_copy2, \
         patch('shutil.copytree') as mock_copytree, \
         patch('shutil.rmtree') as mock_rmtree:
        mock_copy2.return_value = None
        mock_copytree.return_value = None
        mock_rmtree.return_value = None
        yield (mock_copy2, mock_copytree, mock_rmtree)
        
@pytest.fixture
def mock_os_makedirs():
    """Mock os.makedirs for testing"""
    with patch('os.makedirs') as mock_makedirs:
        mock_makedirs.return_value = None
        yield mock_makedirs

# New fixtures for ETL pipeline DAG testing

@pytest.fixture
def mock_glue_job_operator():
    """Mock GlueJobOperator for testing"""
    with patch('airflow.providers.amazon.aws.operators.glue.GlueJobOperator.execute') as mock_execute:
        mock_execute.return_value = {'JobRunId': 'test-job-run-123'}
        yield mock_execute

@pytest.fixture
def mock_glue_client():
    """Mock boto3 Glue client for testing"""
    with patch('boto3.client') as mock_client:
        # Create a mock specifically for Glue service
        def client_side_effect(service_name, *args, **kwargs):
            if service_name == 'glue':
                mock_glue = MagicMock()
                # Set up expected responses for common Glue operations
                mock_glue.start_job_run.return_value = {
                    'JobRunId': 'test-job-run-123'
                }
                mock_glue.get_job_run.return_value = {
                    'JobRun': {
                        'JobRunState': 'SUCCEEDED',
                        'CompletedOn': '2025-04-11T00:00:00+00:00',
                        'StartedOn': '2025-04-10T00:00:00+00:00',
                        'JobName': 'test-job'
                    }
                }
                return mock_glue
            elif service_name == 's3':
                return mock_s3_client()
            else:
                return MagicMock()
                
        mock_client.side_effect = client_side_effect
        yield mock_client

@pytest.fixture
def mock_slack_notify():
    """Mock slack_notify module for testing failure callback"""
    with patch('slack_notify.send_failure_alert') as mock_notify:
        mock_notify.return_value = None
        yield mock_notify

@pytest.fixture
def mock_dummy_operator():
    """Mock DummyOperator for testing"""
    with patch('airflow.operators.dummy_operator.DummyOperator.execute') as mock_execute:
        mock_execute.return_value = None
        yield mock_execute