from unittest.mock import (
    MagicMock,
    patch,
)

import pandas as pd
import pytest
from products import (
    clean_nulls_and_empty_values,
    clean_products_data,
    clean_special_characters,
    convert_product_id,
    filter_valid_products,
    load_products_data,
    normalize_column_names,
    write_to_csv,
)
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing."""
    return (
        SparkSession.builder.master("local[1]").appName("PySpark-Testing").getOrCreate()
    )


@pytest.fixture
def sample_data(spark_session):
    """Create sample data for testing."""
    data = [
        ("001", "Product A", "Category 1"),
        ("002 units", "Product B", "Category 2"),
        ("003", "N/A", "Category 3"),
        ("004", "Product D", "Unknown"),
        ("005", "Product E", None),
        ("006#", "Product F@", "Category |6"),
        (None, "Product G", "Category 7"),
    ]

    schema = StructType(
        [
            StructField("PRODUCT_ID", StringType(), True),
            StructField("product.name", StringType(), True),
            StructField("category", StringType(), True),
        ]
    )

    return spark_session.createDataFrame(data, schema)


def test_normalize_column_names(spark_session, sample_data):
    """Test normalizing column names."""
    result = normalize_column_names(sample_data)
    assert "product_id" in result.columns
    assert "product_name" in result.columns
    assert "category" in result.columns
    assert result.count() == sample_data.count()


def test_clean_nulls_and_empty_values(spark_session, sample_data):
    """Test cleaning null and empty values."""
    normalized_df = normalize_column_names(sample_data)
    result = clean_nulls_and_empty_values(normalized_df)
    pandas_df = result.toPandas()
    assert pandas_df.loc[2, "product_name"] is None
    assert pandas_df.loc[3, "category"] is None
    assert pandas_df.loc[4, "category"] is None


def test_convert_product_id(spark_session, sample_data):
    """Test converting product_id."""
    normalized_df = normalize_column_names(sample_data)
    null_cleaned_df = clean_nulls_and_empty_values(normalized_df)
    result = convert_product_id(null_cleaned_df)
    pandas_df = result.toPandas()
    assert len(pandas_df) < len(null_cleaned_df.toPandas())
    assert all(pd.notna(pandas_df["product_id"]))
    assert 2 in pandas_df["product_id"].values


def test_clean_special_characters(spark_session, sample_data):
    """Test cleaning special characters."""
    normalized_df = normalize_column_names(sample_data)
    null_cleaned_df = clean_nulls_and_empty_values(normalized_df)
    id_cleaned_df = convert_product_id(null_cleaned_df)
    result = clean_special_characters(id_cleaned_df)
    pandas_df = result.toPandas()
    row_with_special_chars = pandas_df[pandas_df["product_id"] == 6]

    if not row_with_special_chars.empty:
        assert "#" not in str(row_with_special_chars["product_id"].iloc[0])
        assert "@" not in row_with_special_chars["product_name"].iloc[0]
        assert "|" not in row_with_special_chars["category"].iloc[0]


def test_filter_valid_products(spark_session, sample_data):
    """Test filtering valid products."""
    normalized_df = normalize_column_names(sample_data)
    null_cleaned_df = clean_nulls_and_empty_values(normalized_df)
    id_cleaned_df = convert_product_id(null_cleaned_df)
    special_char_cleaned_df = clean_special_characters(id_cleaned_df)
    result = filter_valid_products(special_char_cleaned_df)
    pandas_df = result.toPandas()
    assert pandas_df["product_id"].notna().all()
    assert pandas_df["product_name"].notna().all()
    assert pandas_df["category"].notna().all()


def test_clean_products_data(spark_session, sample_data):
    """Test the complete cleaning process."""
    result = clean_products_data(sample_data)
    assert set(result.columns) == {"product_id", "product_name", "category"}
    pandas_df = result.toPandas()
    assert pandas_df["product_id"].notna().all()
    assert pandas_df["product_name"].notna().all()
    assert pandas_df["category"].notna().all()
