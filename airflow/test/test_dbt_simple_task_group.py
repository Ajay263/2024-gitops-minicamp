import pytest
from airflow.models import DagBag
import os

class TestNexabrandsDbtIncrementalDag:
    """Tests specific to the nexabrands_dbt_incremental_dag DAG"""
    
    @pytest.fixture
    def nexabrands_dag(self, dagbag):
        """Fixture that provides the nexabrands_dbt_incremental_dag"""
        dag_id = "nexabrands_dbt_incremental_dag"
        return dagbag.get_dag(dag_id)
    
    def test_dag_loaded(self, nexabrands_dag):
        """Test that the DAG is loaded"""
        assert nexabrands_dag is not None, "DAG not found"
        assert nexabrands_dag.dag_id == "nexabrands_dbt_incremental_dag", f"DAG ID is {nexabrands_dag.dag_id}, expected 'nexabrands_dbt_incremental_dag'"
    
    def test_dag_structure(self, nexabrands_dag):
        """Test the structure of the DAG"""
        # Check for expected tasks
        task_ids = [task.task_id for task in nexabrands_dag.tasks]
        
        expected_tasks = [
            "pre_dbt_workflow",
            "source_freshness_check",
            "staging_models",
            "marts_models",
            "post_dbt_workflow"
        ]
        
        for task in expected_tasks:
            assert task in task_ids or any(task in t for t in task_ids), f"Task {task} not found in DAG"
        
        # Check that the task groups exist (they'll have prefixed task IDs)
        staging_tasks = [task for task in task_ids if task.startswith("staging_models.")]
        assert len(staging_tasks) > 0, "No staging model tasks found"
        
        marts_tasks = [task for task in task_ids if task.startswith("marts_models.")]
        assert len(marts_tasks) > 0, "No marts model tasks found"
    
    def test_task_dependencies(self, nexabrands_dag):
        """Test task dependencies in the DAG"""
        # Get main tasks
        pre_workflow = nexabrands_dag.get_task("pre_dbt_workflow")
        source_freshness = nexabrands_dag.get_task("source_freshness_check")
        post_workflow = nexabrands_dag.get_task("post_dbt_workflow")
        
        # Find the first task in each task group
        staging_tasks = [task for task in nexabrands_dag.tasks if task.task_id.startswith("staging_models.")]
        marts_tasks = [task for task in nexabrands_dag.tasks if task.task_id.startswith("marts_models.")]
        
        # Verify flow: pre_workflow >> source_freshness >> staging_models >> marts_models >> post_workflow
        assert "source_freshness_check" in pre_workflow.downstream_task_ids, "pre_workflow should lead to source_freshness"
        
        # Check that source_freshness leads to staging models
        downstream_ids = set()
        for task_id in source_freshness.downstream_task_ids:
            downstream_ids.add(task_id)
        
        assert any(task_id.startswith("staging_models.") for task_id in downstream_ids), "source_freshness should lead to staging models"
        
        # Check that staging models lead to marts models
        staging_downstream = set()
        for task in staging_tasks:
            for task_id in task.downstream_task_ids:
                staging_downstream.add(task_id)
        
        assert any(task_id.startswith("marts_models.") for task_id in staging_downstream), "staging models should lead to marts models"
        
        # Check that marts models lead to post_workflow
        marts_downstream = set()
        for task in marts_tasks:
            for task_id in task.downstream_task_ids:
                marts_downstream.add(task_id)
        
        assert "post_dbt_workflow" in marts_downstream, "marts models should lead to post_workflow"
    
    def test_source_freshness_task(self, nexabrands_dag, mock_bash_operator):
        """Test the source freshness task"""
        task = nexabrands_dag.get_task("source_freshness_check")
        # Execute the task
        task.execute(context={})
        # Verify mock was called
        mock_bash_operator.assert_called_once()
    
    def test_dag_parameters(self, nexabrands_dag):
        """Test DAG parameters are properly configured"""
        # Test default arguments
        assert nexabrands_dag.default_args["owner"] == "airflow", "Owner should be 'airflow'"
        assert nexabrands_dag.default_args["retries"] == 2, "Retries should be 2"
        assert "on_failure_callback" in nexabrands_dag.default_args, "Missing failure callback"
        
        # Test parameters
        params = nexabrands_dag.params
        assert "start_time" in params, "Missing start_time parameter"
        assert "end_time" in params, "Missing end_time parameter"
    
    def test_dag_schedule(self, nexabrands_dag):
        """Test that the DAG has the expected schedule"""
        assert nexabrands_dag.schedule_interval == "@hourly", "Schedule should be '@hourly'"
        
    def test_task_group_configuration(self, nexabrands_dag):
        """Test that task groups are configured correctly"""
        # Find all tasks in the staging group
        staging_tasks = [task for task in nexabrands_dag.tasks if task.task_id.startswith("staging_models.")]
        
        # Check that there's at least one task
        assert len(staging_tasks) > 0, "No tasks found in staging_models group"
        
        # Same for marts group
        marts_tasks = [task for task in nexabrands_dag.tasks if task.task_id.startswith("marts_models.")]
        assert len(marts_tasks) > 0, "No tasks found in marts_models group"