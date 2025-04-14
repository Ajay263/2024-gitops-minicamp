import pytest
from airflow.models import DagBag
import os


class TestDbtDocsGeneratorDag:
    """Tests specific to the dbt_docs_generator DAG"""

    def test_dag_loaded(self, dag):
        """Test that the DAG is loaded"""
        assert dag is not None, "DAG not found"
        assert dag.dag_id == "dbt_docs_generator", f"DAG ID is {dag.dag_id}, expected 'dbt_docs_generator'"

    def test_dag_structure(self, dag):
        """Test the structure of the DAG"""
        # Check for expected tasks
        task_ids = [task.task_id for task in dag.tasks]
        
        expected_tasks = [
            "verify_redshift_connection",
            "dbt_setup.install_dbt_deps",
            "generate_dbt_docs",
            "upload_docs_to_s3"
        ]
        
        for task in expected_tasks:
            assert task in task_ids, f"Task {task} not found in DAG"

    def test_task_dependencies(self, dag):
        """Test task dependencies in the DAG"""
        # Get all tasks
        tasks = {task.task_id: task for task in dag.tasks}
        
        # Check dependencies
        connection_check = tasks["verify_redshift_connection"]
        install_deps = tasks["dbt_setup.install_dbt_deps"]
        generate_docs = tasks["generate_dbt_docs"]
        upload_docs = tasks["upload_docs_to_s3"]
        
        # Verify the flow: connection_check >> setup >> generate_docs >> upload_docs
        assert connection_check.downstream_task_ids == {"dbt_setup.install_dbt_deps"}
        assert "generate_dbt_docs" in install_deps.downstream_task_ids
        assert "upload_docs_to_s3" in generate_docs.downstream_task_ids

    def test_verify_redshift_connection_task(self, dag, mock_redshift_conn, mock_psycopg2_connect):
        """Test the verify_redshift_connection task"""
        task = dag.get_task("verify_redshift_connection")
        # Execute the task - this should use our mocked connection
        task.execute(context={})
        # Verify our mock was called with expected parameters
        mock_psycopg2_connect.assert_called_once()

    def test_upload_docs_to_s3_task(self, dag, mock_s3_client, mock_os_path_exists, 
                                   mock_os_listdir, mock_os_path_isdir):
        """Test the upload_docs_to_s3 task"""
        task = dag.get_task("upload_docs_to_s3")
        # Execute the task
        task.execute(context={})
        
        # Verify S3 upload was called (at least once)
        assert mock_s3_client.upload_file.call_count > 0, "S3 upload was not called"
        
        # Verify S3 upload is using encryption
        args, kwargs = mock_s3_client.upload_file.call_args_list[0]
        assert 'ExtraArgs' in kwargs
        assert 'ServerSideEncryption' in kwargs['ExtraArgs']
        assert kwargs['ExtraArgs']['ServerSideEncryption'] == 'AES256'

    def test_save_docs_locally_function(self, dag, mock_os_path_exists, mock_shutil, mock_os_makedirs):
        """Test the save_docs_locally function"""
        from dags.dbt_docs_generator import save_docs_locally
        
        # Call the function
        save_docs_locally("/test/project/dir", "/test/output/dir")
        
        # Verify makedirs was called with expected parameters
        mock_os_makedirs.assert_called_once_with("/test/output/dir", exist_ok=True)
        
        # Verify copies happened (rest of the test remains the same)
        mock_copy2, mock_copytree, mock_rmtree = mock_shutil
        assert mock_copy2.call_count > 0, "No files were copied"
        assert mock_copytree.call_count > 0, "Assets directory was not copied"