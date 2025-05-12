from datetime import date
from unittest.mock import (
    MagicMock,
    patch,
)

import numpy as np
import pandas as pd
import pytest
from order_lines import (
    add_derived_columns,
    clean_actual_delivery_date,
    clean_agreed_delivery_date,
    clean_order_id_and_product_id,
    clean_order_lines_data,
    clean_order_qty_and_delivery_qty,
    convert_column_names_to_lowercase,
    drop_null_values,
    filter_invalid_quantities,
    filter_unwanted_values,
    load_order_lines_data,
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
        SparkSession.builder.master("local[*]").appName("OrderLinesTests").getOrCreate()
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
def sample_order_lines_df(spark_session):
    """Create a sample DataFrame for testing order lines data."""
    data = [
        ("ORD-123", "P456", 10.0, "01/15/2024", "01/20/2024", "8"),
        ("ord 456", "prod789", 5.0, "2024-02-10", "2024-02-15", "5"),
        ("ORD/789", "P-101", 0.0, "03/01/2023", "03/05/2023", "0"),
        ("ORD#012", "P202", 20.0, "invalid-date", "04/10/2024", "15"),
        ("ORD-345", "P303", 30.0, "05/01/2024", "invalid-date", "25"),
        ("NULL", "P404", 40.0, "06/01/2024", "06/05/2024", "35"),
        ("ORD-567", "NULL", 50.0, "07/01/2024", "07/05/2024", "45"),
        (None, "P606", 60.0, "08/01/2024", "08/05/2024", "55"),
        ("ORD-678", None, 70.0, "09/01/2024", "09/05/2024", "65"),
        ("ORD-789", "P808", None, "10/01/2024", "10/05/2024", "75"),
        ("ORD-890", "P909", 90.0, None, "11/05/2024", "85"),
        ("ORD-901", "P010", 100.0, "12/01/2024", None, "95"),
        ("ORD-123", "P111", 110.0, "01/01/2024", "01/05/2024", None),
    ]
    schema = StructType(
        [
            StructField("ORDER_ID", StringType(), True),
            StructField("PRODUCT_ID", StringType(), True),
            StructField("ORDER_QTY", FloatType(), True),
            StructField("AGREED_DELIVERY_DATE", StringType(), True),
            StructField("ACTUAL_DELIVERY_DATE", StringType(), True),
            StructField("DELIVERY_QTY", StringType(), True),
        ]
    )
    return spark_session.createDataFrame(data=data, schema=schema)


def test_load_order_lines_data(spark_session, mock_glue_context):
    """Test that load_order_lines_data correctly loads CSV data with the specified schema."""
    test_data = pd.DataFrame(
        {
            "ORDER_ID": ["ORD-123", "ORD-456"],
            "PRODUCT_ID": ["P123", "P456"],
            "ORDER_QTY": [10.0, 20.0],
            "AGREED_DELIVERY_DATE": ["01/15/2024", "02/15/2024"],
            "ACTUAL_DELIVERY_DATE": ["01/20/2024", "02/20/2024"],
            "DELIVERY_QTY": ["8", "18"],
        }
    )
    csv_path = "/tmp/test_order_lines.csv"
    test_data.to_csv(csv_path, index=False)

    with patch("boto3.client") as mock_boto:
        result_df = load_order_lines_data(mock_glue_context, csv_path)
        assert len(result_df.schema) == 6
        assert "ORDER_ID" in result_df.columns
        assert "PRODUCT_ID" in result_df.columns
        assert "ORDER_QTY" in result_df.columns
        assert "AGREED_DELIVERY_DATE" in result_df.columns
        assert "ACTUAL_DELIVERY_DATE" in result_df.columns
        assert "DELIVERY_QTY" in result_df.columns
        assert result_df.count() == 2
        assert result_df.filter(result_df.ORDER_ID == "ORD-123").count() == 1
        assert result_df.schema["ORDER_QTY"].dataType == FloatType()
        assert result_df.schema["ORDER_ID"].dataType == StringType()


def test_clean_order_qty_and_delivery_qty(spark_session, sample_order_lines_df):
    """Test that ORDER_QTY and DELIVERY_QTY are properly cleaned."""
    result_df = clean_order_qty_and_delivery_qty(sample_order_lines_df)

    assert result_df.schema["ORDER_QTY"].dataType == IntegerType()

    assert result_df.schema["DELIVERY_QTY"].dataType == IntegerType()

    row = result_df.filter(result_df.ORDER_ID == "ORD-123").collect()[0]
    assert row["ORDER_QTY"] == 10
    assert row["DELIVERY_QTY"] == 8


def test_filter_invalid_quantities(spark_session):
    """Test that rows with invalid quantities are filtered out."""
    data = [
        ("ORD-1", "P1", 10, "2024-01-01", "2024-01-05", 8), 
        ("ORD-2", "P2", 0, "2024-01-01", "2024-01-05", 5), 
        ("ORD-3", "P3", 15, "2024-01-01", "2024-01-05", 0),  
        ("ORD-4", "P4", -5, "2024-01-01", "2024-01-05", 10), 
        ("ORD-5", "P5", 20, "2024-01-01", "2024-01-05", -8), 
    ]
    schema = StructType(
        [
            StructField("ORDER_ID", StringType(), True),
            StructField("PRODUCT_ID", StringType(), True),
            StructField("ORDER_QTY", IntegerType(), True),
            StructField("AGREED_DELIVERY_DATE", StringType(), True),
            StructField("ACTUAL_DELIVERY_DATE", StringType(), True),
            StructField("DELIVERY_QTY", IntegerType(), True),
        ]
    )
    test_df = spark_session.createDataFrame(data=data, schema=schema)
    result_df = filter_invalid_quantities(test_df)
    assert result_df.count() == 1
    assert result_df.collect()[0]["ORDER_ID"] == "ORD-1"


def test_filter_unwanted_values(spark_session):
    """Test that rows with unwanted values are filtered out."""
    data = [
        ("ORD-1", "P1", 10, "2024-01-01", "2024-01-05", 8),  
        ("NULL", "P2", 15, "2024-01-01", "2024-01-05", 12),  
        ("ORD-3", "null", 20, "2024-01-01", "2024-01-05", 18),  
        ("ORD-4", "P4", 25, "NA", "2024-01-05", 22), 
        ("ORD-5", "P5", 30, "2024-01-01", "none", 28),  
        ("ORD-6", "P6", 35, "2024-01-01", "2024-01-05", "N/A"), 
    ]
    schema = StructType(
        [
            StructField("ORDER_ID", StringType(), True),
            StructField("PRODUCT_ID", StringType(), True),
            StructField("ORDER_QTY", IntegerType(), True),
            StructField("AGREED_DELIVERY_DATE", StringType(), True),
            StructField("ACTUAL_DELIVERY_DATE", StringType(), True),
            StructField("DELIVERY_QTY", StringType(), True),
        ]
    )
    test_df = spark_session.createDataFrame(data=data, schema=schema)
    unwanted_values = ["NULL", "null", "NA", "none", "N/A"]
    result_df = filter_unwanted_values(test_df, unwanted_values)
    assert result_df.count() == 1
    assert result_df.collect()[0]["ORDER_ID"] == "ORD-1"


def test_drop_null_values(spark_session):
    """Test that rows with null values are dropped."""
    data = [
        ("ORD-1", "P1", 10, "2024-01-01", "2024-01-05", 8),  
        (None, "P2", 15, "2024-01-01", "2024-01-05", 12),  
        ("ORD-3", None, 20, "2024-01-01", "2024-01-05", 18),  
        ("ORD-4", "P4", None, "2024-01-01", "2024-01-05", 22),  
        ("ORD-5", "P5", 30, None, "2024-01-05", 28),  
        ("ORD-6", "P6", 35, "2024-01-01", None, 32), 
        ("ORD-7", "P7", 40, "2024-01-01", "2024-01-05", None),  
    ]
    schema = StructType(
        [
            StructField("ORDER_ID", StringType(), True),
            StructField("PRODUCT_ID", StringType(), True),
            StructField("ORDER_QTY", IntegerType(), True),
            StructField("AGREED_DELIVERY_DATE", StringType(), True),
            StructField("ACTUAL_DELIVERY_DATE", StringType(), True),
            StructField("DELIVERY_QTY", IntegerType(), True),
        ]
    )
    test_df = spark_session.createDataFrame(data=data, schema=schema)
    result_df = drop_null_values(test_df)
    assert result_df.count() == 1
    assert result_df.collect()[0]["ORDER_ID"] == "ORD-1"


def test_convert_column_names_to_lowercase(spark_session):
    """Test that column names are converted to lowercase."""
    data = [("ORD-1", "P1", 10)]
    schema = StructType(
        [
            StructField("ORDER_ID", StringType(), True),
            StructField("Product_ID", StringType(), True),
            StructField("order_QTY", IntegerType(), True),
        ]
    )
    test_df = spark_session.createDataFrame(data=data, schema=schema)
    result_df = convert_column_names_to_lowercase(test_df)
    columns = result_df.columns
    assert "order_id" in columns
    assert "product_id" in columns
    assert "order_qty" in columns
    assert "ORDER_ID" not in columns
    assert "Product_ID" not in columns
    assert "order_QTY" not in columns


def test_add_derived_columns(spark_session):
    """Test that derived columns are correctly added."""
    data = [
        ("ORD-1", "P1", 10, date(2024, 1, 1), date(2024, 1, 1), 10),
        ("ORD-2", "P2", 15, date(2024, 1, 1), date(2024, 1, 5), 15),
        ("ORD-3", "P3", 20, date(2024, 1, 1), date(2024, 1, 1), 15),
        ("ORD-4", "P4", 25, date(2024, 1, 10), date(2024, 1, 5), 25),
        ("ORD-5", "P5", 30, date(2024, 1, 10), date(2024, 1, 5), 20),
    ]
    schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("order_qty", IntegerType(), True),
            StructField("agreed_delivery_date", DateType(), True),
            StructField("actual_delivery_date", DateType(), True),
            StructField("delivery_qty", IntegerType(), True),
        ]
    )
    test_df = spark_session.createDataFrame(data=data, schema=schema)
    result_df = add_derived_columns(test_df)
    assert "delivery_delay_days" in result_df.columns
    assert "delivery_completion_rate" in result_df.columns
    assert "is_on_time" in result_df.columns
    assert "is_complete_delivery" in result_df.columns

    rows = result_df.collect()
    assert rows[0]["delivery_delay_days"] == 0
    assert rows[0]["delivery_completion_rate"] == 100.0
    assert rows[0]["is_on_time"] == "Yes"
    assert rows[0]["is_complete_delivery"] == "Yes"
    assert rows[1]["delivery_delay_days"] == 4
    assert rows[1]["delivery_completion_rate"] == 100.0
    assert rows[1]["is_on_time"] == "No"
    assert rows[1]["is_complete_delivery"] == "Yes"
    assert rows[2]["delivery_delay_days"] == 0
    assert rows[2]["delivery_completion_rate"] == 75.0
    assert rows[2]["is_on_time"] == "Yes"
    assert rows[2]["is_complete_delivery"] == "No"
    assert rows[3]["delivery_delay_days"] == -5
    assert rows[3]["delivery_completion_rate"] == 100.0
    assert rows[3]["is_on_time"] == "Yes"
    assert rows[3]["is_complete_delivery"] == "Yes"
    assert rows[4]["delivery_delay_days"] == -5
    assert abs(rows[4]["delivery_completion_rate"] - 66.67) < 0.01
    assert rows[4]["is_on_time"] == "Yes"
    assert rows[4]["is_complete_delivery"] == "No"


def test_clean_order_id_empty_string(spark_session):
    """Test handling of empty strings in ORDER_ID."""
    data = [("", "P1", 10.0, "01/15/2024", "01/20/2024", "8")]
    schema = StructType(
        [
            StructField("ORDER_ID", StringType(), True),
            StructField("PRODUCT_ID", StringType(), True),
            StructField("ORDER_QTY", FloatType(), True),
            StructField("AGREED_DELIVERY_DATE", StringType(), True),
            StructField("ACTUAL_DELIVERY_DATE", StringType(), True),
            StructField("DELIVERY_QTY", StringType(), True),
        ]
    )
    test_df = spark_session.createDataFrame(data=data, schema=schema)
    result_df = clean_order_id_and_product_id(test_df)
    assert result_df.collect()[0]["ORDER_ID"] == ""


def test_clean_product_id_with_invalid_format(spark_session):
    """Test handling of PRODUCT_ID with completely non-numeric content."""
    data = [("ORD-1", "ABC", 10.0, "01/15/2024", "01/20/2024", "8")]
    schema = StructType(
        [
            StructField("ORDER_ID", StringType(), True),
            StructField("PRODUCT_ID", StringType(), True),
            StructField("ORDER_QTY", FloatType(), True),
            StructField("AGREED_DELIVERY_DATE", StringType(), True),
            StructField("ACTUAL_DELIVERY_DATE", StringType(), True),
            StructField("DELIVERY_QTY", StringType(), True),
        ]
    )
    test_df = spark_session.createDataFrame(data=data, schema=schema)
    result_df = clean_order_id_and_product_id(test_df)
    assert result_df.collect()[0]["PRODUCT_ID"] is None


def test_filter_invalid_quantities_with_null_values(spark_session):
    """Test filter_invalid_quantities with null values in quantities."""
    data = [
        ("ORD-1", "P1", 10, "2024-01-01", "2024-01-05", 8),  
        ("ORD-2", "P2", None, "2024-01-01", "2024-01-05", 5),  
        ("ORD-3", "P3", 15, "2024-01-01", "2024-01-05", None), 
    ]
    schema = StructType(
        [
            StructField("ORDER_ID", StringType(), True),
            StructField("PRODUCT_ID", StringType(), True),
            StructField("ORDER_QTY", IntegerType(), True),
            StructField("AGREED_DELIVERY_DATE", StringType(), True),
            StructField("ACTUAL_DELIVERY_DATE", StringType(), True),
            StructField("DELIVERY_QTY", IntegerType(), True),
        ]
    )
    test_df = spark_session.createDataFrame(data=data, schema=schema)
    result_df = filter_invalid_quantities(test_df)
    assert result_df.count() == 1
    assert result_df.collect()[0]["ORDER_ID"] == "ORD-1"


def test_add_derived_columns_with_null_dates(spark_session):
    """Test add_derived_columns with null dates."""
    data = [
        ("ORD-1", "P1", 10, None, date(2024, 1, 5), 8),  
        ("ORD-2", "P2", 15, date(2024, 1, 1), None, 12), 
    ]
    schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("order_qty", IntegerType(), True),
            StructField("agreed_delivery_date", DateType(), True),
            StructField("actual_delivery_date", DateType(), True),
            StructField("delivery_qty", IntegerType(), True),
        ]
    )
    test_df = spark_session.createDataFrame(data=data, schema=schema)
    result_df = add_derived_columns(test_df)
    rows = result_df.collect()
    assert rows[0]["delivery_delay_days"] is None
    assert rows[1]["delivery_delay_days"] is None
    assert rows[0]["delivery_completion_rate"] == 80.0
    assert rows[0]["is_complete_delivery"] == "No"
    assert rows[1]["delivery_completion_rate"] == 80.0
    assert rows[1]["is_complete_delivery"] == "No"
