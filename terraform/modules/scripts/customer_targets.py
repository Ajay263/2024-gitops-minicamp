import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import (
    col,
    regexp_replace,
    when,
)
from pyspark.sql.types import (
    FloatType,
    StringType,
    StructField,
    StructType,
)


def load_customer_targets_data(
    glue_context: GlueContext, s3_input_path: str
) -> DataFrame:
    """Load customer targets data from a CSV file in S3 using GlueContext."""
    schema = StructType(
        [
            StructField("CUSTOMER_ID", StringType(), True),
            StructField("ontime_target%", FloatType(), True),
            StructField("infull_target%", FloatType(), True),
            StructField("OTIF_TARGET%", FloatType(), True),
        ]
    )
    return (
        glue_context.spark_session.read.format("csv")
        .option("header", True)
        .schema(schema)
        .load(s3_input_path)
    )


def clean_customer_id(df: DataFrame) -> DataFrame:
    """Clean the CUSTOMER_ID column."""
    return df.withColumn(
        "CUSTOMER_ID",
        regexp_replace(col("CUSTOMER_ID"), "(ID_| units)", "").cast("long"),
    )


def handle_negative_values(df: DataFrame) -> DataFrame:
    """Replace negative values in target columns with None."""
    target_columns = ["ontime_target%", "infull_target%", "OTIF_TARGET%"]
    for column in target_columns:
        df = df.withColumn(column, when(col(column) < 0, None).otherwise(col(column)))
    return df


def drop_invalid_rows(df: DataFrame) -> DataFrame:
    """Drop rows with invalid values."""
    # First clean the CUSTOMER_ID
    df = clean_customer_id(df)
    return df.dropna().filter(
        (col("ontime_target%") > 0)
        & (col("infull_target%") > 0)
        & (col("OTIF_TARGET%") > 0)
        & (col("CUSTOMER_ID") > 0)
    )


def rename_columns_to_lowercase(df: DataFrame) -> DataFrame:
    """Rename columns to lowercase and remove special characters."""
    column_mapping = {
        "CUSTOMER_ID": "customer_id",
        "ontime_target%": "ontime_target",
        "infull_target%": "infull_target",
        "OTIF_TARGET%": "otif_target",
    }
    for old_name, new_name in column_mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df


def clean_customer_targets_data(df: DataFrame) -> DataFrame:
    """Clean and transform customer targets data."""
    df = clean_customer_id(df)
    df = handle_negative_values(df)
    df = drop_invalid_rows(df)
    df = rename_columns_to_lowercase(df)
    return df


def write_transformed_data(df: DataFrame, s3_output_path: str) -> None:
    """Write the transformed data to an S3 bucket as a single CSV file."""
    # Coalesce the DataFrame into a single partition to ensure one output file
    df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
        s3_output_path
    )


if __name__ == "__main__":
    # Initialize Spark session and Glue context
    spark = SparkSession.builder.appName("CustomerTargetsDataProcessing").getOrCreate()
    glue_context = GlueContext(spark.sparkContext)
    job = Job(glue_context)
    job.init("customer-targets-processing-job")

    # S3 paths
    s3_input_path = (
        "s3://nexabrand-prod-source/data/customer_targets.csv"  # Input file path
    )
    s3_output_folder = "s3://nexabrand-prod-target/customer_targets/"  # Output folder
    s3_temp_output_path = f"{s3_output_folder}temp/"  # Temporary output path

    # Load and clean data
    customer_targets_df = load_customer_targets_data(glue_context, s3_input_path)
    cleaned_customer_targets = clean_customer_targets_data(customer_targets_df)

    # Save the cleaned data to S3 as a single CSV file in a temporary folder
    write_transformed_data(cleaned_customer_targets, s3_temp_output_path)

    # Use boto3 to rename the file to `customer_targets.csv`
    s3_client = boto3.client("s3")
    bucket_name = "nexabrand-prod-target"  # Output bucket name

    # Find the generated CSV file in the temporary folder.
    response = s3_client.list_objects_v2(
        Bucket=bucket_name, Prefix="customer_targets/temp/"
    )
    if "Contents" in response:
        for obj in response["Contents"]:
            if obj["Key"].endswith(".csv"):
                source_key = obj["Key"]
                # Construct the destination key
                destination_key = "customer_targets/customer_targets.csv"
                # Copy the file to the new location
                copy_source = {"Bucket": bucket_name, "Key": source_key}
                s3_client.copy_object(
                    CopySource=copy_source, Bucket=bucket_name, Key=destination_key
                )
                # Delete the original file
                s3_client.delete_object(Bucket=bucket_name, Key=source_key)

    # Delete the temporary folder
    s3_client.delete_object(Bucket=bucket_name, Key="customer_targets/temp/")

    # Commit the Glue job
    job.commit()
