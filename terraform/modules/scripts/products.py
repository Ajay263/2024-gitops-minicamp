import sys

import boto3
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    regexp_replace,
    trim,
    when,
)


from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)


def load_products_data(spark_session, file_path: str) -> DataFrame:
    """
    Load products data from a CSV file.

    Args:
        spark_session: The Spark session.
        file_path (str): The S3 path to the input CSV file.

    Returns:
        DataFrame: A Spark DataFrame containing the loaded products data.
    """
    schema = StructType(
        [
            StructField("PRODUCT_ID", StringType(), True),
            StructField("product.name", StringType(), True),
            StructField("category", StringType(), True),
        ]
    )
    return (
        spark_session.read.format("csv")
        .option("header", True)
        .schema(schema)
        .load(file_path)
    )


def normalize_column_names(df: DataFrame) -> DataFrame:
    """
    Normalize column names to standardized format.

    Args:
        df (DataFrame): A Spark DataFrame with original column names.

    Returns:
        DataFrame: DataFrame with normalized column names.
    """
    return df.selectExpr(
        "PRODUCT_ID as product_id", "`product.name` as product_name", "category"
    )


def clean_nulls_and_empty_values(df: DataFrame) -> DataFrame:
    """
    Clean null and empty values in the DataFrame.

    Args:
        df (DataFrame): DataFrame to clean.

    Returns:
        DataFrame: Cleaned DataFrame.
    """
    return df.withColumn(
        "category",
        when(
            (col("category").isNull())
            | (trim(col("category")).isin("", "NULL", "Unknown", "N/A")),
            None,
        ).otherwise(trim(col("category"))),
    ).withColumn(
        "product_name",
        when(
            (col("product_name").isNull())
            | (trim(col("product_name")).isin("N/A", "NULL", "Unknown")),
            None,
        ).otherwise(trim(col("product_name"))),
    )


def convert_product_id(df: DataFrame) -> DataFrame:
    """
    Convert product_id to proper format and drop rows with null product_id.

    Args:
        df (DataFrame): DataFrame with product_id to clean.

    Returns:
        DataFrame: DataFrame with cleaned product_id.
    """
    return df.withColumn(
        "product_id", regexp_replace(col("product_id"), " units", "").cast("int")
    ).dropna(subset=["product_id"])


def clean_special_characters(df: DataFrame) -> DataFrame:
    """
    Remove special characters from specified columns.

    Args:
        df (DataFrame): DataFrame to clean.

    Returns:
        DataFrame: DataFrame with special characters removed.
    """
    cleaned_df = df
    for column in ["product_name", "category", "product_id"]:
        cleaned_df = cleaned_df.withColumn(
            column, trim(regexp_replace(col(column), r"[|#@$]", ""))
        )
    return cleaned_df


def filter_valid_products(df: DataFrame) -> DataFrame:
    """
    Filter out records with null values in required fields.

    Args:
        df (DataFrame): DataFrame to filter.

    Returns:
        DataFrame: Filtered DataFrame.
    """
    return df.filter(
        (col("product_id").isNotNull())
        & (col("product_name").isNotNull())
        & (col("category").isNotNull())
    )


def clean_products_data(df: DataFrame) -> DataFrame:
    """
    Clean and transform products data.

    Args:
        df (DataFrame): A Spark DataFrame containing raw products data.

    Returns:
        DataFrame: A cleaned and transformed Spark DataFrame.
    """
    products_df = normalize_column_names(df)
    products_df = clean_nulls_and_empty_values(products_df)
    products_df = convert_product_id(products_df)
    products_df = clean_special_characters(products_df)
    products_df = filter_valid_products(products_df)
    return products_df


def write_to_csv(df: DataFrame, output_path: str) -> None:
    """
    Write DataFrame to CSV.

    Args:
        df (DataFrame): DataFrame to write.
        output_path (str): Path where the CSV should be saved.

    Returns:
        None
    """
    df.coalesce(1).write.mode("overwrite").option("header", "true").option(
        "quote", '"'
    ).option("escape", '"').csv(output_path)

    print(
        f"Products ETL job completed successfully. CSV output saved to: {output_path}"
    )


if __name__ == "__main__":
    # Initialize Spark and Glue context
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    # Define input and output paths
    input_path = "s3a://nexabrands-prod-source/data/products.csv"
    output_bucket = "nexabrands-prod-target"
    output_path = f"s3a://{output_bucket}/products/products.csv"

    # Load and clean data
    products_df = load_products_data(spark, input_path)
    cleaned_products = clean_products_data(products_df)

    # Write as single CSV file
    write_to_csv(cleaned_products, output_path)
