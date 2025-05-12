# test_customer_targets.py

import sys
from unittest.mock import (
    MagicMock,
    patch,
)

import pytest
from customer_targets import (
    clean_customer_id,
    clean_customer_targets_data,
    drop_invalid_rows,
    handle_negative_values,
    load_customer_targets_data,
    rename_columns_to_lowercase,
)
from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import col
from pyspark.sql.types import (
    FloatType,
    StringType,
    StructField,
    StructType,
)


@pytest.fixture(scope="session")
def spark_session():
    """Fixture to create a Spark session for testing."""
    spark = (
        SparkSession.builder.appName("pytest-pyspark").master("local[*]").getOrCreate()
    )
    yield spark
    spark.stop()


def test_load_customer_targets_data(spark_session):
    """Test loading customer targets data."""
    mock_df = spark_session.createDataFrame(
        [("ID_123 units", 95.0, 90.0, 85.0)],
        schema=["CUSTOMER_ID", "ontime_target%", "infull_target%", "OTIF_TARGET%"],
    )

    mock_glue_context = MagicMock()
    mock_glue_context.spark_session.read.format.return_value.option.return_value.schema.return_value.load.return_value = (
        mock_df
    )

    with patch("customer_targets.GlueContext", return_value=mock_glue_context):
        df = load_customer_targets_data(mock_glue_context, "s3a://test-bucket/test.csv")
        assert isinstance(df, DataFrame)
        assert df.columns == [
            "CUSTOMER_ID",
            "ontime_target%",
            "infull_target%",
            "OTIF_TARGET%",
        ]


def test_clean_customer_id(spark_session):
    """Test cleaning the CUSTOMER_ID column."""
    df = spark_session.createDataFrame(
        [("ID_123 units", 95.0, 90.0, 85.0)],
        schema=["CUSTOMER_ID", "ontime_target%", "infull_target%", "OTIF_TARGET%"],
    )
    cleaned_df = clean_customer_id(df)
    assert cleaned_df.filter(col("CUSTOMER_ID") == 123).count() == 1


def test_handle_negative_values(spark_session):
    """Test replacing negative values in target columns with None."""
    df = spark_session.createDataFrame(
        [("ID_123 units", -95.0, 90.0, 85.0)],
        schema=["CUSTOMER_ID", "ontime_target%", "infull_target%", "OTIF_TARGET%"],
    )
    cleaned_df = handle_negative_values(df)
    assert cleaned_df.filter(col("ontime_target%").isNull()).count() == 1


def test_drop_invalid_rows(spark_session):
    """Test dropping rows with invalid values."""
    df = spark_session.createDataFrame(
        [("ID_123 units", 95.0, 90.0, 85.0), ("ID_456 units", -95.0, -90.0, -85.0)],
        schema=["CUSTOMER_ID", "ontime_target%", "infull_target%", "OTIF_TARGET%"],
    )
    cleaned_df = drop_invalid_rows(df)
    assert cleaned_df.count() == 1


def test_rename_columns_to_lowercase(spark_session):
    """Test renaming columns to lowercase."""
    df = spark_session.createDataFrame(
        [("ID_123 units", 95.0, 90.0, 85.0)],
        schema=["CUSTOMER_ID", "ontime_target%", "infull_target%", "OTIF_TARGET%"],
    )
    renamed_df = rename_columns_to_lowercase(df)
    assert renamed_df.columns == [
        "customer_id",
        "ontime_target",
        "infull_target",
        "otif_target",
    ]


def test_clean_customer_targets_data(spark_session):
    """Test cleaning and transforming customer targets data."""
    df = spark_session.createDataFrame(
        [("ID_123 units", 95.0, 90.0, 85.0), ("ID_456 units", -95.0, -90.0, -85.0)],
        schema=["CUSTOMER_ID", "ontime_target%", "infull_target%", "OTIF_TARGET%"],
    )
    cleaned_df = clean_customer_targets_data(df)
    assert cleaned_df.count() == 1
    row = cleaned_df.collect()[0]
    assert row.customer_id == 123
    assert row.ontime_target == 95.0
    assert row.infull_target == 90.0
    assert row.otif_target == 85.0
