
# File 2: full_refresh_dag.py
import os

import pendulum
import slack_notify  
from airflow.decorators import dag
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import (
    LoadMode,
    TestBehavior,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping
import slack_notify  

# Profile configuration
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="airflow_db",
        profile_args={"schema": "public"},
    ),
)

# Project configuration
dbt_project_path = f"{os.environ['AIRFLOW_HOME']}/dbt/nexabrands_dbt"
dbt_executable_path = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

project_config = ProjectConfig(
    dbt_project_path=dbt_project_path,
    manifest_path=f"{os.environ['AIRFLOW__COSMOS__DBT_DOCS_DIR']}/manifest.json",
    dbt_vars={
        "start_time": "{{ params.start_time if params.start_time is not none else data_interval_start }}",
        "end_time": "{{ params.end_time if params.end_time is not none else data_interval_end }}",
    },
    partial_parse=False,
)

# Execution configuration
execution_config = ExecutionConfig(
    dbt_executable_path=dbt_executable_path,
)

default_args = {
    "owner": "airflow",
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
    "on_failure_callback": slack_notify.send_failure_alert ,
    'params': {
        'start_time': Param(None, type=["null", "string"], format="date", description="Start date for run", title="Start date"),
        'end_time': Param(None, type=["null", "string"], format="date", description="End date for run", title="End date"),
    }
}

@dag(
    schedule_interval=None,  # Manual triggers only
    start_date=pendulum.datetime(2024, 7, 1),
    catchup=False,
    tags=["dbt", "full-refresh"],
    max_active_runs=1,
    max_active_tasks=5,
    default_args=default_args
)
def nexabrands_dbt_full_refresh_dag() -> None:
    """
    Full refresh DBT DAG for complete data reprocessing
    """
    pre_dbt_workflow = EmptyOperator(task_id="pre_dbt_workflow")

    source_freshness = BashOperator(
        task_id="source_freshness_check",
        bash_command=f"cd {dbt_project_path} && {dbt_executable_path} source freshness --profiles-dir {dbt_project_path}",
    )

    snapshots = DbtTaskGroup(
        group_id="snapshots",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=["path:snapshots"],
            test_behavior=TestBehavior.AFTER_ALL,
            dbt_deps=False,
        ),
    )

    staging_models = DbtTaskGroup(
        group_id="staging_models",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "full_refresh": True,  # Always do full refresh
        },
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=["path:models/staging"],
            test_behavior=TestBehavior.AFTER_EACH,
            dbt_deps=False,
        ),
    )

    marts_models = DbtTaskGroup(
        group_id="marts_models",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "full_refresh": True,  # Always do full refresh
        },
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=["path:models/marts"],
            exclude=["path:models/marts/intermediate"],
            test_behavior=TestBehavior.AFTER_EACH,
            dbt_deps=False,
        ),
    )

    core_models = DbtTaskGroup(
        group_id="core_models",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "full_refresh": True,  # Always do full refresh
        },
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=["path:models/core"],
            test_behavior=TestBehavior.AFTER_EACH,
            dbt_deps=False,
        ),
    )

    post_dbt_workflow = EmptyOperator(task_id="post_dbt_workflow")

    pre_dbt_workflow >> source_freshness >> snapshots >> staging_models >> marts_models >> core_models >> post_dbt_workflow

dag = nexabrands_dbt_full_refresh_dag()
