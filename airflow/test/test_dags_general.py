import glob
import importlib.util
import logging
import os
from contextlib import contextmanager

import pytest
from airflow.models import DAG, DagBag


@contextmanager
def suppress_logging(namespace):
    """
    Context manager to temporarily disable logging for a specified namespace.
    
    Args:
        namespace (str): The logging namespace to suppress.
    """
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


@pytest.fixture
def dagbag():
    """
    Fixture that returns a DagBag instance with example DAGs excluded.
    """
    with suppress_logging("airflow"):
        return DagBag(include_examples=False)


def get_import_errors():
    """
    Generate a tuple for import errors in the dag bag.
    
    Returns:
        list: List of tuples containing (relative_path, error_message).
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)
        
    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME", ""))
        
    # prepend "(None,None)" to ensure that a test object is always created even if it's a no op.
    return [(None, None)] + [
        (strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()
    ]


def get_dags():
    """
    Generate a tuple of dag_id, <DAG objects> in the DagBag.
    
    Returns:
        list: List of tuples containing (dag_id, dag_object, file_location).
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)
        
    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME", ""))
        
    return [(k, v, strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]


class TestDagsGeneral:
    """
    Tests that apply to all DAGs to ensure they meet project standards
    and best practices.
    """
    
    def test_dagbag_import_errors(self, dagbag):
        """
        Test that there are no DAG import errors.
        
        Import errors usually indicate syntax errors or missing dependencies
        that prevent the DAG from being properly loaded.
        """
        assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"
    
    def test_dag_count(self, dagbag):
        """
        Test that we have the expected number of DAGs.
        
        This test helps detect when DAGs are accidentally removed or added
        without updating the expected count.
        """
        # Replace the expected count with the actual number of your DAGs
        expected_dag_count = 3  # Adjust based on your project
        assert len(dagbag.dags) == expected_dag_count, f"Expected {expected_dag_count} DAGs, got {len(dagbag.dags)}"
    
    def test_all_dags_have_tags(self, dagbag):
        """
        Test that all DAGs have tags.
        
        Tags help with organization and filtering in the Airflow UI and
        are considered a best practice for DAG management.
        """
        for dag_id, dag in dagbag.dags.items():
            assert dag.tags, f"DAG {dag_id} has no tags"
    
    def test_all_dags_have_description(self, dagbag):
        """
        Test that all DAGs have descriptions.
        
        Descriptions provide context about what a DAG does and are
        important for documentation purposes.
        """
        for dag_id, dag in dagbag.dags.items():
            assert dag.description, f"DAG {dag_id} has no description"
    
    def test_all_dags_have_owners(self, dagbag):
        """
        Test that all DAGs have owners.
        
        Owner information helps identify who is responsible for
        a DAG in case of issues or questions.
        """
        for dag_id, dag in dagbag.dags.items():
            assert dag.owner, f"DAG {dag_id} has no owner"
    
    def test_all_dags_have_start_dates(self, dagbag):
        """
        Test that all DAGs have start dates.
        
        Start dates are required for scheduling and determine when
        a DAG should begin execution.
        """
        for dag_id, dag in dagbag.dags.items():
            assert dag.start_date, f"DAG {dag_id} has no start date"
    
    def test_no_duplicate_task_ids(self, dagbag):
        """
        Test that there are no duplicate task IDs within DAGs.
        
        Duplicate task IDs within a DAG can cause conflicts and confusion
        when monitoring or debugging workflows.
        """
        for dag_id, dag in dagbag.dags.items():
            task_ids = [task.task_id for task in dag.tasks]
            assert len(task_ids) == len(set(task_ids)), f"DAG {dag_id} has duplicate task IDs"
    
    def test_no_cycles(self, dagbag):
        """
        Test that there are no cycles in DAGs.
        
        Cycles in DAG task dependencies would create infinite loops,
        which Airflow doesn't allow. This test ensures the DAG is properly
        structured as a directed acyclic graph.
        """
        for dag_id, dag in dagbag.dags.items():
            # This will raise an exception if there's a cycle
            dag.topological_sort()


# Define approved tags if needed
APPROVED_TAGS = {}  # Add your approved tags here if you want to restrict tag usage


@pytest.mark.parametrize(
    "rel_path,rv", get_import_errors(), ids=[x[0] for x in get_import_errors()]
)
def test_file_imports(rel_path, rv):
    """
    Test for import errors on a file.
    
    This test checks each DAG file for import errors and provides detailed
    error messages if problems are found.
    """
    if rel_path and rv:
        raise Exception(f"{rel_path} failed to import with message \n {rv}")


@pytest.mark.parametrize(
    "dag_id,dag,fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_tags(dag_id, dag, fileloc):
    """
    Test if a DAG is tagged and if those tags are in the approved list.
    
    Tags help with organization in the Airflow UI. This test ensures all
    DAGs have tags and optionally checks that they are from an approved list.
    """
    assert dag.tags, f"{dag_id} in {fileloc} has no tags"
    if APPROVED_TAGS:
        assert not set(dag.tags) - APPROVED_TAGS, f"{dag_id} has tags not in the approved list: {set(dag.tags) - APPROVED_TAGS}"


@pytest.mark.parametrize(
    "dag_id,dag,fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_retries(dag_id, dag, fileloc):
    """
    Test if a DAG has retries set to an appropriate value.
    
    Retries help with task resilience. This test ensures tasks will retry
    at least twice before failing permanently.
    """
    assert (
        dag.default_args.get("retries", None) >= 2
    ), f"{dag_id} in {fileloc} must have task retries >= 2."


# Get all DAG files using glob pattern
DAG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "dags/**/*.py"
)
DAG_FILES = glob.glob(DAG_PATH, recursive=True)


@pytest.mark.parametrize("dag_file", DAG_FILES)
def test_dag_integrity(dag_file):
    """
    Test the integrity of each DAG file by dynamically loading it.
    
    This test checks that:
    1. The file can be imported without errors
    2. The file contains at least one DAG object
    3. Each DAG doesn't contain cycles
    
    This is a thorough test that actually loads the DAG code rather than
    relying on the DagBag parser.
    """
    module_name, _ = os.path.splitext(os.path.basename(dag_file))
    module_path = dag_file
    
    mod_spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(mod_spec)
    mod_spec.loader.exec_module(module)
    
    dag_objects = [var for var in vars(module).values() if isinstance(var, DAG)]
    assert dag_objects, f"No DAG objects found in {dag_file}"
    
    for dag in dag_objects:
        dag.test_cycle()  # Verify there are no cycles in the DAG