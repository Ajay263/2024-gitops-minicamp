from unittest.mock import (
    MagicMock,
    patch,
)

import pandas as pd
import pytest
from orders import (
    clean_orders_data,
    load_orders_data,
)
from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import (
    col,
    lit,
)
from pyspark.sql.types import (
    DateType,
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
        .appName("Orders_Unit_Tests")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def sample_orders_df(spark_session):
    """Create a sample DataFrame for testing."""
    data = [
        ("ORD123", "1001", "01/15/2024"),
        ("ord-456", "1002", "02/20/2024"),
        ("ORD789", "1003", "03/25/2024"),
        ("NA", "ID_1004", "04/30/2024"),
        ("ORD321", "1005ABC", "05/05/2024"),
        ("ORD654", "1006", "Monday, 06/10/2023"),
        ("ORD987", "1007", "07/15/2023"),
        ("ORD987", "1007", "07/15/2023"),  
        ("", "NULL", ""), 
        ("Unknown", "none", "N/A"),  
    ]
    schema = StructType(
        [
            StructField("ORDER_ID", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_placement_date", StringType(), True),
        ]
    )
    return spark_session.createDataFrame(data=data, schema=schema)


@pytest.fixture
def expected_schema():
    """Define the expected schema after cleaning."""
    return StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("order_placement_date", DateType(), True),
        ]
    )


def test_load_orders_data(spark_session):
    """Test that load_orders_data correctly loads CSV data with the specified schema."""
    test_data = pd.DataFrame(
        {
            "ORDER_ID": ["ORD123", "ORD456", "ORD789"],
            "customer_id": ["1001", "1002", "1003"],
            "order_placement_date": ["01/15/2024", "02/20/2024", "03/25/2024"],
        }
    )
    csv_path = "/tmp/test_orders.csv"
    test_data.to_csv(csv_path, index=False)

    result_df = load_orders_data(spark_session, csv_path)

    
    
    assert len(result_df.schema) == 3
    assert "ORDER_ID" in result_df.columns
    assert "customer_id" in result_df.columns
    assert "order_placement_date" in result_df.columns


    
    assert result_df.count() == 3
    assert result_df.filter(result_df.ORDER_ID == "ORD123").count() == 1


def test_clean_orders_data_column_renaming(sample_orders_df):
    """Test that columns are properly renamed."""
    result_df = clean_orders_data(sample_orders_df)
    assert "order_id" in result_df.columns
    assert "ORDER_ID" not in result_df.columns


def test_clean_orders_data_unwanted_values(spark_session):
    """Test filtering of unwanted values."""
    data = [
        ("ORD123", "1001", "01/15/2024"),
        ("NA", "1002", "02/20/2024"),
        ("ORD456", "none", "03/25/2024"),
        ("ORD789", "1004", "NULL"),
    ]
    schema = StructType(
        [
            StructField("ORDER_ID", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_placement_date", StringType(), True),
        ]
    )
    test_df = spark_session.createDataFrame(data=data, schema=schema)

    result_df = clean_orders_data(test_df)
    assert result_df.count() == 1  
    
    assert result_df.collect()[0].order_id == "ORD123"


def test_clean_orders_data_customer_id_cleaning(spark_session):
    """Test that customer_id is properly cleaned and converted to integer."""
    data = [
        ("ORD123", "1001", "01/15/2024"),
        ("ORD456", "ID_1002", "02/20/2024"),
        ("ORD789", "1003ABC", "03/25/2024"),
        ("ORD321", "1004", "04/30/2024"),
    ]
    schema = StructType(
        [
            StructField("ORDER_ID", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_placement_date", StringType(), True),
        ]
    )
    test_df = spark_session.createDataFrame(data=data, schema=schema)

    result_df = clean_orders_data(test_df)

    assert result_df.filter(result_df.customer_id == 1001).count() == 1
    assert result_df.filter(result_df.customer_id == 1004).count() == 1
    assert result_df.filter(col("customer_id").like("ID_%")).count() == 0
    assert result_df.filter(col("customer_id").rlike("[^0-9.]")).count() == 0


def test_clean_orders_data_date_cleaning(spark_session):
    """Test that order_placement_date is properly cleaned and standardized."""
    data = [
        ("ORD123", "1001", "01/15/2024"),
        ("ORD456", "1002", "Monday, 02/20/2023"),
        ("ORD789", "1003", "03/25/2023"),
        ("ORD321", "1004", "04/30/2022"),
    ]
    schema = StructType(
        [
            StructField("ORDER_ID", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_placement_date", StringType(), True),
        ]
    )
    test_df = spark_session.createDataFrame(data=data, schema=schema)

    result_df = clean_orders_data(test_df)

    assert all(row.order_placement_date.year == 2024 for row in result_df.collect())

    jan_date = (
        result_df.filter(result_df.order_id == "ORD123")
        .collect()[0]
        .order_placement_date
    )
    assert jan_date.month == 1
    assert jan_date.day == 15

    feb_date = (
        result_df.filter(result_df.order_id == "ORD456")
        .collect()[0]
        .order_placement_date
    )
    assert feb_date.month == 2
    assert feb_date.day == 20


def test_clean_orders_data_drop_duplicates(spark_session):
    """Test that duplicate rows are properly removed."""
    data = [
        ("ORD123", "1001", "01/15/2024"),
        ("ORD123", "1001", "01/15/2024"),  
        ("ORD456", "1002", "02/20/2024"),
        ("ORD456", "1002", "02/20/2024"),  
    ]
    schema = StructType(
        [
            StructField("ORDER_ID", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_placement_date", StringType(), True),
        ]
    )
    test_df = spark_session.createDataFrame(data=data, schema=schema)

    result_df = clean_orders_data(test_df)
    assert result_df.count() == 2  


def test_clean_orders_data_drop_nulls(spark_session):
    """Test that rows with null values are properly removed."""
    data = [
        ("ORD123", "1001", "01/15/2024"),
        ("ORD456", None, "02/20/2024"),
        ("ORD789", "1003", None),
        (None, "1004", "04/30/2024"),
    ]
    schema = StructType(
        [
            StructField("ORDER_ID", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_placement_date", StringType(), True),
        ]
    )
    test_df = spark_session.createDataFrame(data=data, schema=schema)
    result_df = clean_orders_data(test_df)
    assert result_df.count() == 1  
    assert result_df.collect()[0].order_id == "ORD123"
