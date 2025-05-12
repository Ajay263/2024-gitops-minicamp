# test_dates.py
import sys
from unittest.mock import (
    MagicMock,
    patch,
)

import pytest
from dates import (
    clean_dates_data,
    clean_special_characters,
    clean_week_no_column,
    drop_invalid_rows,
    filter_unwanted_values,
    format_date_column,
    format_mmm_yy_column,
    load_dates_data,
    rename_and_lowercase_columns,
)
from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

sys.modules["awsglue"] = MagicMock()
sys.modules["awsglue.context"] = MagicMock()
sys.modules["awsglue.job"] = MagicMock()


@pytest.fixture(scope="session")
def spark_session():
    """Fixture to create a Spark session for testing."""
    spark = (
        SparkSession.builder.appName("pytest-pyspark").master("local[*]").getOrCreate()
    )
    yield spark
    spark.stop()


def test_load_dates_data(spark_session):
    """Test loading dates data."""
    mock_df = spark_session.createDataFrame(
        [("01-Jan-23", "Jan-23", "W1")],
        schema=["DATE", "mmm.yy", "week_no"],
    )

    mock_glue_context = MagicMock()
    mock_glue_context.spark_session.read.format.return_value.option.return_value.schema.return_value.load.return_value = (
        mock_df
    )

    with patch("dates.GlueContext", return_value=mock_glue_context):
        df = load_dates_data(mock_glue_context, "s3a://test-bucket/test.csv")
        assert isinstance(df, DataFrame)
        assert df.columns == ["DATE", "mmm.yy", "week_no"]


def test_rename_and_lowercase_columns(spark_session):
    """Test renaming columns to lowercase and replacing special characters."""
    df = spark_session.createDataFrame(
        [("01-Jan-23", "Jan-23", "W1")],
        schema=["DATE", "mmm.yy", "week_no"],
    )
    renamed_df = rename_and_lowercase_columns(df)
    assert renamed_df.columns == ["date", "mmm_yy", "week_no"]


def test_clean_special_characters(spark_session):
    """Test cleaning special characters from the 'date' and 'mmm_yy' columns."""
    df = spark_session.createDataFrame(
        [("01-Jan-23!", "Jan-23!", "W1")],
        schema=["date", "mmm_yy", "week_no"],
    )
    cleaned_df = clean_special_characters(df)
    assert cleaned_df.filter(col("date") == "01-Jan-23").count() == 1
    assert cleaned_df.filter(col("mmm_yy") == "Jan-23").count() == 1


def test_filter_unwanted_values(spark_session):
    """Test filtering out rows with null or unwanted values."""
    df = spark_session.createDataFrame(
        [("01-Jan-23", "Jan-23", "W1"), ("N/A", "NULL", "unknown")],
        schema=["date", "mmm_yy", "week_no"],
    )
    filtered_df = filter_unwanted_values(df)
    assert filtered_df.count() == 1


def test_format_date_column(spark_session):
    """Test formatting the 'date' column to 'dd-MM-yyyy' format."""
    df = spark_session.createDataFrame(
        [("01-Jan-23", "Jan-23", "W1")],
        schema=["date", "mmm_yy", "week_no"],
    )
    formatted_df = format_date_column(df)
    assert formatted_df.filter(col("date") == "01-01-2024").count() == 1


def test_format_mmm_yy_column(spark_session):
    """Test formatting the 'mmm_yy' column to 'dd-MM-yyyy' format."""
    df = spark_session.createDataFrame(
        [("01-Jan-23", "Jan-23", "W1")],
        schema=["date", "mmm_yy", "week_no"],
    )
    formatted_df = format_mmm_yy_column(df)
    assert formatted_df.filter(col("mmm_yy") == "01-01-2024").count() == 1


def test_clean_week_no_column(spark_session):
    """Test cleaning the 'week_no' column by removing 'W' and casting to integer."""
    df = spark_session.createDataFrame(
        [("01-Jan-23", "Jan-23", "W1")],
        schema=["date", "mmm_yy", "week_no"],
    )
    cleaned_df = clean_week_no_column(df)
    assert cleaned_df.filter(col("week_no") == 1).count() == 1


def test_drop_invalid_rows(spark_session):
    """Test dropping rows with null 'week_no' values."""
    df = spark_session.createDataFrame(
        [("01-Jan-23", "Jan-23", "1"), ("01-Jan-23", "Jan-23", None)],
        schema=["date", "mmm_yy", "week_no"],
    )
    cleaned_df = drop_invalid_rows(df)
    assert cleaned_df.count() == 1


def test_clean_dates_data(spark_session):
    """Test cleaning and transforming dates data."""
    df = spark_session.createDataFrame(
        [("01-Jan-23!", "Jan-23!", "W1")],
        schema=["DATE", "mmm.yy", "week_no"],
    )
    cleaned_df = clean_dates_data(df)
    assert cleaned_df.count() == 1
    row = cleaned_df.collect()[0]
    assert row.date == "01-01-2024"
    assert row.mmm_yy == "01-01-2024"
    assert row.week_no == 1
