from pendulum import datetime
from airflow.decorators import dag
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)

# Update this path to match where your great_expectations directory is located
MY_GX_DATA_CONTEXT = "/opt/airflow/include/gx"  # Adjust if needed

@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["great_expectations", "data_quality"],
)
def customer_targets_validation():
    validate_customer_targets = GreatExpectationsOperator(
        task_id="validate_customer_targets",
        data_context_root_dir=MY_GX_DATA_CONTEXT,
        checkpoint_name="customer_targets_checkpoint",
        fail_task_on_validation_failure=True,
        return_json_dict=True,
    )

customer_targets_validation()