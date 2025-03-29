import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import (
    col,
    initcap,
    regexp_replace,
    trim,
)
from pyspark.sql.types import (
    FloatType,
    StringType,
    StructField,
    StructType,
)


def load_customers_data(glue_context: GlueContext, s3_input_path: str) -> DataFrame:
    """Load customers data from a CSV file in S3 using GlueContext."""
    schema = StructType(
        [
            StructField("CUSTOMER_ID", FloatType(), True),
            StructField("customer_name", StringType(), True),
            StructField("city", StringType(), True),
        ]
    )
    return (
        glue_context.spark_session.read.format("csv")
        .option("header", True)
        .schema(schema)
        .load(s3_input_path)
    )


def drop_na_and_duplicates(df: DataFrame) -> DataFrame:
    """Drop rows with null values and duplicate rows."""
    return df.na.drop().dropDuplicates()


def trim_string_columns(df: DataFrame) -> DataFrame:
    """Trim whitespace from string columns."""
    return df.select(
        [trim(col(c)).alias(c) if t == "string" else col(c) for c, t in df.dtypes]
    )


def clean_customer_id(df: DataFrame) -> DataFrame:
    """Clean and cast the CUSTOMER_ID column."""
    return (
        df.withColumn(
            "CUSTOMER_ID",
            col("CUSTOMER_ID").cast("double"),  # First ensure it's a double
        )
        .filter(
            (col("CUSTOMER_ID").isNotNull())
            & (col("CUSTOMER_ID") > 0)
            & (
                col("CUSTOMER_ID") == col("CUSTOMER_ID").cast("int").cast("double")
            )  # Check if it's a whole number
        )
        .withColumn(
            "CUSTOMER_ID", col("CUSTOMER_ID").cast("int")
        )  # Finally cast to int
    )


def clean_string_columns(df: DataFrame, columns: list) -> DataFrame:
    """Clean and format string columns."""
    for column in columns:
        df = df.withColumn(
            column,
            initcap(  # First apply initcap
                regexp_replace(  # Then replace special characters with spaces
                    regexp_replace(
                        col(column), r"[^a-zA-Z0-9\s]", " "
                    ),  # Replace special chars with space
                    r"\s+",  # Replace multiple spaces with single space
                    " ",
                )
            ),
        )
    return df


def rename_columns_to_lowercase(df: DataFrame) -> DataFrame:
    """Rename all columns to lowercase."""
    return df.select([col(c).alias(c.lower()) for c in df.columns])


def clean_customer_data(df: DataFrame) -> DataFrame:
    """Clean and transform customers data."""
    # First clean customer ID to filter out invalid IDs
    df = clean_customer_id(df)
    # Then proceed with other cleaning steps
    df = drop_na_and_duplicates(df)
    df = trim_string_columns(df)
    df = clean_string_columns(df, ["customer_name", "city"])
    df = rename_columns_to_lowercase(df)
    return df


def write_transformed_data(df: DataFrame, s3_output_path: str) -> None:
    """Write the transformed data to an S3 bucket as a single CSV file."""
    df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
        s3_output_path
    )  # noqa: E501


if __name__ == "__main__":
    # Initialize Spark session and Glue context
    spark = SparkSession.builder.appName("CustomersDataProcessing").getOrCreate()
    glue_context = GlueContext(spark.sparkContext)
    job = Job(glue_context)
    job.init("customers-data-processing-job")

    # S3 paths
    s3_input_path = "s3://nexabrand-prod-source/data/customers.csv"  # Input file path
    s3_output_folder = "s3://nexabrand-prod-target/customers/"  # Output folder
    s3_temp_output_path = f"{s3_output_folder}temp/"  # Temporary output path

    # Load and clean data
    customers_df = load_customers_data(glue_context, s3_input_path)
    cleaned_customers = clean_customer_data(customers_df)

    # Save the cleaned data to S3 as a single CSV file in a temporary folder
    write_transformed_data(cleaned_customers, s3_temp_output_path)

    # Use boto3 to rename the file to `customers.csv`
    s3_client = boto3.client("s3")
    bucket_name = "nexabrand-prod-target"  # Output bucket name

    # Find the generated CSV file in the temporary folder
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="customers/temp/")
    if "Contents" in response:
        for obj in response["Contents"]:
            if obj["Key"].endswith(".csv"):
                source_key = obj["Key"]
                # Construct the destination key
                destination_key = "customers/customers.csv"
                # Copy the file to the new location
                copy_source = {"Bucket": bucket_name, "Key": source_key}
                s3_client.copy_object(
                    CopySource=copy_source, Bucket=bucket_name, Key=destination_key
                )
                # Delete the original file
                s3_client.delete_object(Bucket=bucket_name, Key=source_key)

    # Delete the temporary folder
    s3_client.delete_object(Bucket=bucket_name, Key="customers/temp/")

    # Commit the Glue job
    job.commit()
