from unittest.mock import (
    MagicMock,
    patch,
)

import boto3
import numpy as np
import pandas as pd
import pytest
from customers import (
    clean_customer_data,
    clean_customer_id,
    clean_string_columns,
    drop_na_and_duplicates,
    load_customers_data,
    rename_columns_to_lowercase,
    trim_string_columns,
    write_transformed_data,
)
from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


@pytest.fixture(scope="session")
def spark_session():
    """Create a SparkSession that can be reused across tests."""
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("PySpark_Unit_Tests")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def mock_glue_context(spark_session):
    """Create a mock GlueContext with a reference to the real SparkSession."""
    mock_glue = MagicMock()
    mock_glue.spark_session = spark_session
    return mock_glue


@pytest.fixture
def sample_customers_df(spark_session):
    """Create a sample DataFrame for testing."""
    data = [
        (1.0, "John Doe  ", "New York"),
        (2.0, "Jane Smith", " Chicago"),
        (3.0, "Bob Johnson", "Los Angeles "),
        (None, "Invalid User", "Seattle"),
        (4.5, "Mary Williams", "Boston"),
        (5.0, "Steve@Brown", "Dallas"),
        (5.0, "Steve@Brown", "Dallas"),  
    ]
    schema = StructType(
        [
            StructField("CUSTOMER_ID", FloatType(), True),
            StructField("customer_name", StringType(), True),
            StructField("city", StringType(), True),
        ]
    )
    return spark_session.createDataFrame(data=data, schema=schema)


@pytest.fixture
def expected_schema():
    """Define the expected schema after cleaning."""
    return StructType(
        [
            StructField("customer_id", IntegerType(), True),
            StructField("customer_name", StringType(), True),
            StructField("city", StringType(), True),
        ]
    )


def test_load_customers_data(spark_session, mock_glue_context):
    """Test that load_customers_data correctly loads CSV data with the specified schema."""
    test_data = pd.DataFrame(
        {
            "CUSTOMER_ID": [1.0, 2.0, 3.0],
            "customer_name": ["John Doe", "Jane Smith", "Bob Johnson"],
            "city": ["New York", "Chicago", "Los Angeles"],
        }
    )
    csv_path = "/tmp/test_customers.csv"
    test_data.to_csv(csv_path, index=False)

    with patch("boto3.client") as mock_boto:
        result_df = load_customers_data(mock_glue_context, csv_path)
        assert len(result_df.schema) == 3
        assert "CUSTOMER_ID" in result_df.columns
        assert "customer_name" in result_df.columns
        assert "city" in result_df.columns
        assert result_df.count() == 3
        assert result_df.filter(result_df.CUSTOMER_ID == 1.0).count() == 1


def test_drop_na_values(spark_session):
    """Test that null values are dropped correctly."""
    data = [
        (1.0, "John", "NY"),
        (2.0, None, "LA"),
        (3.0, "Bob", None),
        (None, "Alice", "SF"),
    ]
    schema = StructType(
        [
            StructField("CUSTOMER_ID", FloatType(), True),
            StructField("customer_name", StringType(), True),
            StructField("city", StringType(), True),
        ]
    )
    test_df = spark_session.createDataFrame(data, schema)
    result_df = drop_na_and_duplicates(test_df)
    assert result_df.count() == 1
    assert result_df.collect()[0][0] == 1.0


def test_drop_duplicates(spark_session):
    """Test that duplicate rows are dropped correctly."""
    data = [
        (1.0, "John", "NY"),
        (1.0, "John", "NY"),
        (2.0, "Jane", "LA"),
        (2.0, "Jane", "LA"),
    ]
    schema = StructType(
        [
            StructField("CUSTOMER_ID", FloatType(), True),
            StructField("customer_name", StringType(), True),
            StructField("city", StringType(), True),
        ]
    )
    test_df = spark_session.createDataFrame(data, schema)
    result_df = drop_na_and_duplicates(test_df)
    assert result_df.count() == 2


def test_trim_string_columns(spark_session):
    """Test that string columns are properly trimmed of whitespace."""
    data = [(1.0, "  John  ", "  NY  "), (2.0, "Jane", "LA"), (3.0, " Bob ", "SF ")]
    schema = StructType(
        [
            StructField("CUSTOMER_ID", FloatType(), True),
            StructField("customer_name", StringType(), True),
            StructField("city", StringType(), True),
        ]
    )
    test_df = spark_session.createDataFrame(data, schema)
    result_df = trim_string_columns(test_df)
    trimmed_data = result_df.collect()
    assert trimmed_data[0][1] == "John"
    assert trimmed_data[0][2] == "NY"
    assert trimmed_data[2][1] == "Bob"
    assert trimmed_data[2][2] == "SF"


def test_clean_customer_id_valid_ids(spark_session):
    """Test that valid customer IDs are properly handled."""
    data = [(1.0, "John", "NY"), (2.0, "Jane", "LA"), (3.0, "Bob", "SF")]
    schema = StructType(
        [
            StructField("CUSTOMER_ID", FloatType(), True),
            StructField("customer_name", StringType(), True),
            StructField("city", StringType(), True),
        ]
    )
    test_df = spark_session.createDataFrame(data, schema)
    result_df = clean_customer_id(test_df)
    cleaned_data = result_df.collect()
    assert result_df.count() == 3
    assert isinstance(cleaned_data[0][0], int)
    assert cleaned_data[0][0] == 1


def test_clean_customer_id_invalid_ids(spark_session):
    """Test that invalid customer IDs are filtered out."""
    data = [
        (1.0, "John", "NY"),
        (-2.0, "Jane", "LA"),
        (3.5, "Bob", "SF"),
        (None, "Alice", "DC"),
        (0.0, "Charlie", "Miami"),
    ]
    schema = StructType(
        [
            StructField("CUSTOMER_ID", FloatType(), True),
            StructField("customer_name", StringType(), True),
            StructField("city", StringType(), True),
        ]
    )
    test_df = spark_session.createDataFrame(data, schema)
    result_df = clean_customer_id(test_df)
    assert result_df.count() == 1
    assert result_df.collect()[0][0] == 1


def test_rename_columns_to_lowercase(spark_session):
    """Test that all column names are converted to lowercase."""
    data = [(1, "John", "NY")]
    schema = StructType(
        [
            StructField("CUSTOMER_ID", IntegerType(), True),
            StructField("Customer_Name", StringType(), True),
            StructField("city", StringType(), True),
        ]
    )
    test_df = spark_session.createDataFrame(data, schema)
    result_df = rename_columns_to_lowercase(test_df)
    columns = result_df.columns
    assert "customer_id" in columns
    assert "customer_name" in columns
    assert "city" in columns
    assert "CUSTOMER_ID" not in columns
    assert "Customer_Name" not in columns


def test_clean_customer_data_integration(sample_customers_df, expected_schema):
    """Integration test for the entire data cleaning pipeline."""
    result_df = clean_customer_data(sample_customers_df)
    for field in expected_schema:
        assert field.name in result_df.columns
    assert result_df.schema["customer_id"].dataType == IntegerType()
    cleaned_data = result_df.collect()

    assert result_df.count() == 4

    john_row = result_df.filter(result_df.customer_id == 1).collect()[0]
    assert john_row.customer_name == "John Doe"
    assert john_row.city == "New York"
    steve_row = result_df.filter(result_df.customer_id == 5).collect()[0]
    assert steve_row.customer_name == "Steve Brown"  # @ replaced with space


def test_write_transformed_data(spark_session, sample_customers_df):
    """Test that data is correctly written to the specified path."""
    test_output_path = "/tmp/test_output/"
    with patch("boto3.client") as mock_boto:
        write_transformed_data(sample_customers_df, test_output_path)

        result_df = (
            spark_session.read.format("csv")
            .option("header", "true")
            .load(test_output_path)
        )
        assert result_df.count() == len(sample_customers_df.collect())
        for col in sample_customers_df.columns:
            assert col in result_df.columns


def test_clean_string_columns_empty_strings(spark_session):
    """Test handling of empty strings in clean_string_columns."""
    data = [(1.0, "", ""), (2.0, "Jane", "")]
    schema = StructType(
        [
            StructField("CUSTOMER_ID", FloatType(), True),
            StructField("customer_name", StringType(), True),
            StructField("city", StringType(), True),
        ]
    )
    test_df = spark_session.createDataFrame(data, schema)
    columns_to_clean = ["customer_name", "city"]
    result_df = clean_string_columns(test_df, columns_to_clean)
    cleaned_data = result_df.collect()
    assert cleaned_data[0][1] == "" 
    assert cleaned_data[0][2] == "" 
