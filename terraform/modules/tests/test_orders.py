import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col  
from orders import load_orders_data, clean_orders_data

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.appName("pytest-pyspark").getOrCreate()
    yield spark
    spark.stop()

def test_load_orders_data(spark):
    # Create a test DataFrame
    data = [("1", "101", "01/01/2023"), ("2", "102", "02/01/2023")]
    schema = StructType([
        StructField("ORDER_ID", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_placement_date", StringType(), True),
    ])
    test_df = spark.createDataFrame(data, schema)
    
    # Write to a temporary CSV file
    test_csv_path = "test_orders.csv"
    test_df.write.mode("overwrite").option("header", True).csv(test_csv_path)
    
    # Load the data using the function
    loaded_df = load_orders_data(spark, test_csv_path)
    
    # Assert that the loaded data matches the expected schema and data
    assert loaded_df.columns == ["ORDER_ID", "customer_id", "order_placement_date"]
    assert loaded_df.count() == 2

def test_clean_orders_data(spark):
    # Create a test DataFrame with some dirty data
    data = [
        ("1", "101", "01/01/2023"),
        ("2", "NA", "02/01/2023"),
        ("3", "102", "N/A"),
        ("4", "103", "03/01/2023"),
        ("5", "104", "04/01/2023"),
    ]
    schema = StructType([
        StructField("ORDER_ID", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_placement_date", StringType(), True),
    ])
    test_df = spark.createDataFrame(data, schema)
    
    # Clean the data
    cleaned_df = clean_orders_data(test_df)
    
    # Assert that the cleaned data is as expected
    assert cleaned_df.count() == 3  # Only 3 rows should remain after cleaning
    assert cleaned_df.filter(col("customer_id").isNull()).count() == 0
    assert cleaned_df.filter(col("order_placement_date").isNull()).count() == 0

def test_clean_orders_data_with_unwanted_values(spark):
    # Create a test DataFrame with unwanted values
    data = [
        ("1", "101", "01/01/2023"),
        ("2", "none", "02/01/2023"),
        ("3", "102", "NULL"),
        ("4", "103", "03/01/2023"),
        ("5", "104", "04/01/2023"),
    ]
    schema = StructType([
        StructField("ORDER_ID", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_placement_date", StringType(), True),
    ])
    test_df = spark.createDataFrame(data, schema)
    
    # Clean the data
    cleaned_df = clean_orders_data(test_df)
    
    # Assert that the cleaned data is as expected
    assert cleaned_df.count() == 3  # Only 3 rows should remain after cleaning

def test_clean_orders_data_with_invalid_dates(spark):
    # Create a test DataFrame with invalid dates
    data = [
        ("1", "101", "01/01/2023"),
        ("2", "102", "2023-01-02"),  # Invalid format
        ("3", "103", "03/01/2023"),
        ("4", "104", "04/01/2023"),
    ]
    schema = StructType([
        StructField("ORDER_ID", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_placement_date", StringType(), True),
    ])
    test_df = spark.createDataFrame(data, schema)
    
    # Clean the data
    cleaned_df = clean_orders_data(test_df)
    
    # Assert that the cleaned data is as expected
    assert cleaned_df.count() == 3  # Only 3 rows should remain after cleaning

def test_clean_orders_data_with_duplicates(spark):
    # Create a test DataFrame with duplicates
    data = [
        ("1", "101", "01/01/2023"),
        ("1", "101", "01/01/2023"),  # Duplicate
        ("2", "102", "02/01/2023"),
    ]
    schema = StructType([
        StructField("ORDER_ID", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_placement_date", StringType(), True),
    ])
    test_df = spark.createDataFrame(data, schema)
    
    # Clean the data
    cleaned_df = clean_orders_data(test_df)
    
    # Assert that the cleaned data is as expected
    assert cleaned_df.count() == 2  # Duplicates should be removed