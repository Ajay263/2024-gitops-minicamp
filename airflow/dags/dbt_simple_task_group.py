import os

import pendulum
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
from cosmos.profiles import RedshiftUserPasswordProfileMapping
import slack_notify  

# Profile configuration for Redshift
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

# Project configuration
dbt_project_path = f"{os.environ['AIRFLOW_HOME']}/dbt/nexabrands_dbt"
dbt_executable_path = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

project_config = ProjectConfig(
    dbt_project_path=dbt_project_path,
    manifest_path=f"/opt/airflow/dbt-docs/manifest.json",
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
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": slack_notify.send_failure_alert,
    "params": {
        "start_time": Param(
            None,
            type=["null", "string"],
            format="date",
            description="Start date for run",
            title="Start date",
        ),
        "end_time": Param(
            None,
            type=["null", "string"],
            format="date",
            description="End date for run",
            title="End date",
        ),
    },
}


@dag(
    schedule_interval="@hourly",
    start_date=pendulum.datetime(2024, 7, 1),
    catchup=False,
    tags=["dbt", "incremental"],
    max_active_runs=1,
    max_active_tasks=5,
    default_args=default_args,
    doc_md="""
    Incremental DBT DAG for routine transformations.
    
    This DAG processes data incrementally, running the DBT models in stages:
    1. Source freshness checks
    2. Staging models
    3. Marts models
    
    The pipeline is configured to handle incremental loads with custom date parameters.
    """,
)
def nexabrands_dbt_incremental_dag() -> None:
    """
    Incremental DBT DAG for routine transformations
    """
    pre_dbt_workflow = EmptyOperator(task_id="pre_dbt_workflow")

    source_freshness = BashOperator(
        task_id="source_freshness_check",
        bash_command=f"cd {dbt_project_path} && {dbt_executable_path} source freshness --profiles-dir {dbt_project_path}",
    )

    # snapshots = DbtTaskGroup(
    #     group_id="snapshots",
    #     project_config=project_config,
    #     profile_config=profile_config,
    #     execution_config=execution_config,
    #     render_config=RenderConfig(
    #         load_method=LoadMode.DBT_MANIFEST,
    #         select=["path:snapshots"],
    #         test_behavior=TestBehavior.AFTER_ALL,
    #         dbt_deps=False,
    #     ),
    # )

    staging_models = DbtTaskGroup(
        group_id="staging_models",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=["path:models/staging"],
            test_behavior=TestBehavior.AFTER_EACH,
            dbt_deps=False,
        ),
    )

    # intermediate_models = DbtTaskGroup(
    #     group_id="intermediate_models",
    #     project_config=project_config,
    #     profile_config=profile_config,
    #     execution_config=execution_config,
    #     render_config=RenderConfig(
    #         load_method=LoadMode.DBT_MANIFEST,
    #         select=["path:models/intermediate"],
    #         test_behavior=TestBehavior.AFTER_EACH,
    #         dbt_deps=False,
    #     ),
    # )

    marts_models = DbtTaskGroup(
        group_id="marts_models",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=["path:models/marts"],
            exclude=["path:models/marts/intermediate"],
            test_behavior=TestBehavior.AFTER_EACH,
            dbt_deps=False,
        ),
    )

    post_dbt_workflow = EmptyOperator(task_id="post_dbt_workflow")

    (
        pre_dbt_workflow
        >> source_freshness
        >> staging_models
        >> marts_models
        >> post_dbt_workflow
    )


dag = nexabrands_dbt_incremental_dag()
