import pytest
from airflow.models import DagBag
from unittest.mock import patch, MagicMock

class TestEtlPipelineDag:
    """Tests specific to the ETL Pipeline DAG"""
    
    def test_dag_loaded(self, dagbag):
        """Test that the DAG is loaded without errors"""
        assert "etl_pipeline" in dagbag.dags, "DAG not found in DagBag"
        assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"
        
    def test_dag_attributes(self, etl_pipeline_dag):
        """Test the basic attributes of the DAG"""
        assert etl_pipeline_dag is not None, "DAG not found"
        assert etl_pipeline_dag.dag_id == "etl_pipeline", f"DAG ID is {etl_pipeline_dag.dag_id}, expected 'etl_pipeline'"
        assert etl_pipeline_dag.description == 'ETL pipeline for TopDevs data processing'
        assert etl_pipeline_dag.schedule_interval == '0 0 * * *'
        assert etl_pipeline_dag.catchup is False, "Catchup should be False"
        assert etl_pipeline_dag.max_active_runs == 1, "Max active runs should be 1"
        
        # Check tags
        expected_tags = ['etl', 'glue', 'topdevs']
        assert set(etl_pipeline_dag.tags) == set(expected_tags), f"Expected tags {expected_tags}, got {etl_pipeline_dag.tags}"
        
    def test_dag_default_args(self, etl_pipeline_dag):
        """Test the default_args of the DAG"""
        default_args = etl_pipeline_dag.default_args
        assert default_args['owner'] == 'airflow'
        assert default_args['depends_on_past'] is False
        assert default_args['email_on_failure'] is True
        assert default_args['email_on_retry'] is False
        assert default_args['retries'] == 2
        assert default_args['retry_delay'].total_seconds() == 300  # 5 minutes
        assert 'on_failure_callback' in default_args
        
    def test_dag_structure(self, etl_pipeline_dag):
        """Test the structure of the DAG"""
        # Check for expected tasks
        expected_task_ids = [
            'start',
            'end',
            'glue_job_products',
            'glue_job_customers',
            'glue_job_customer_targets',
            'glue_job_orders',
            'glue_job_order_lines',
            'glue_job_order_fulfillment'
        ]
        
        task_ids = [task.task_id for task in etl_pipeline_dag.tasks]
        for task_id in expected_task_ids:
            assert task_id in task_ids, f"Task {task_id} not found in DAG"
            
        # Check total number of tasks
        assert len(etl_pipeline_dag.tasks) == len(expected_task_ids), f"Expected {len(expected_task_ids)} tasks, got {len(etl_pipeline_dag.tasks)}"
    
    def test_glue_job_configurations(self, etl_pipeline_dag):
        """Test the configuration of Glue job tasks"""
        # Test job configurations for each Glue job
        job_names = ['products', 'customers', 'customer_targets', 'orders', 'order_lines', 'order_fulfillment']
        
        for job_name in job_names:
            task_id = f"glue_job_{job_name}"
            task = etl_pipeline_dag.get_task(task_id)
            
            # Check task type
            assert task.__class__.__name__ == "GlueJobOperator", f"Task {task_id} is not a GlueJobOperator"
            
            # Check Glue job parameters
            env = "prod"  # As defined in the DAG
            assert task.job_name == f"topdevs-{env}-{job_name}-job"
            assert task.region_name == 'us-east-1'
            assert task.wait_for_completion is True
            assert task.num_of_dpus == 2
            
            # Check script arguments
            expected_args = {
                "--source-path": f"s3://nexabrand-{env}-source/",
                "--destination-path": f"s3://nexabrand-{env}-target/",
                "--job-name": f"topdevs-{env}-{job_name}-job",
                "--enable-auto-scaling": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics": "true",
                "--environment": env
            }
            
            for arg_key, arg_value in expected_args.items():
                assert task.script_args[arg_key] == arg_value, f"Script arg {arg_key} for {task_id} has incorrect value"
    
    def test_task_dependencies(self, etl_pipeline_dag):
        """Test task dependencies in the DAG"""
        # Define the expected dependencies based on job_configs in the DAG
        expected_dependencies = {
            'start': ['glue_job_products', 'glue_job_customers'],
            'glue_job_products': ['glue_job_orders'],
            'glue_job_customers': ['glue_job_customer_targets', 'glue_job_orders'],
            'glue_job_customer_targets': ['end'],
            'glue_job_orders': ['glue_job_order_lines'],
            'glue_job_order_lines': ['glue_job_order_fulfillment'],
            'glue_job_order_fulfillment': ['end']
        }
        
        # Check each task's downstream tasks
        for upstream_task_id, downstream_task_ids in expected_dependencies.items():
            upstream_task = etl_pipeline_dag.get_task(upstream_task_id)
            for downstream_task_id in downstream_task_ids:
                assert downstream_task_id in upstream_task.downstream_task_ids, (
                    f"Task {downstream_task_id} is not downstream of {upstream_task_id}"
                )
    
    def test_terminal_tasks(self, etl_pipeline_dag):
        """Test that terminal tasks connect to the end task"""
        # Terminal tasks are those that don't have any tasks depending on them
        expected_terminal_tasks = ['glue_job_customer_targets', 'glue_job_order_fulfillment']
        
        # Get the end task
        end_task = etl_pipeline_dag.get_task('end')
        
        # Check that expected terminal tasks have the end task as downstream
        for task_id in expected_terminal_tasks:
            task = etl_pipeline_dag.get_task(task_id)
            assert 'end' in task.downstream_task_ids, f"Terminal task {task_id} should have 'end' as downstream"
        
        # Check end task has no downstream tasks
        assert len(end_task.downstream_task_ids) == 0, "End task should have no downstream tasks"
        
    def test_glue_job_execution(self, etl_pipeline_dag, mock_glue_job_operator):
        """Test the execution of a Glue job task"""
        # Get a glue job task
        task = etl_pipeline_dag.get_task('glue_job_products')
        
        # Execute the task with a mock context
        context = {'task_instance': MagicMock()}
        result = task.execute(context)
        
        # Verify the mock was called and returned expected result
        mock_glue_job_operator.assert_called_once()
        assert result == {'JobRunId': 'test-job-run-123'}, "GlueJobOperator execution returned unexpected result"

    def test_failure_callback(self, mock_slack_notify):
        """Test that the failure callback is properly configured"""
        # Import the DAG module to access its default_args
        from airflow.models import TaskInstance
        import slack_notify
        
        # Create a mock context for the failure callback
        context = {
            'dag': MagicMock(),
            'task_instance': MagicMock(spec=TaskInstance),
            'execution_date': MagicMock(),
            'exception': Exception("Test exception")
        }
        
        # Call the actual function
        slack_notify.send_failure_alert(context)
        
        # Verify the mock was called
        mock_slack_notify.assert_called_once()