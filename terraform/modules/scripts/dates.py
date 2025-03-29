import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import (
    col,
    concat_ws,
    date_format,
    lit,
    regexp_replace,
    to_date,
    when,
)
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)


def load_dates_data(glue_context: GlueContext, s3_input_path: str) -> DataFrame:
    """Load dates data from a CSV file in S3 using GlueContext."""
    schema = StructType(
        [
            StructField("DATE", StringType(), True),
            StructField("mmm.yy", StringType(), True),
            StructField("week_no", StringType(), True),
        ]
    )
    return (
        glue_context.spark_session.read.format("csv")
        .option("header", True)
        .schema(schema)
        .load(s3_input_path)
    )


def rename_and_lowercase_columns(df: DataFrame) -> DataFrame:
    """Rename 'mmm.yy' to 'mmm_yy' and lowercase all column names."""
    df = df.withColumnRenamed("mmm.yy", "mmm_yy")
    return df.select([col(c).alias(c.lower()) for c in df.columns])


def clean_special_characters(df: DataFrame) -> DataFrame:
    """Remove special characters and spaces from the 'date' and 'mmm_yy' columns."""
    return df.withColumn(
        "date", regexp_replace(col("date"), r"[^a-zA-Z0-9-]", "")
    ).withColumn("mmm_yy", regexp_replace(col("mmm_yy"), r"[^a-zA-Z0-9-]", ""))


def filter_unwanted_values(df: DataFrame) -> DataFrame:
    """Filter out rows with null or unwanted values."""
    unwanted_values = ["NONE", "NULL", "null", "unknown", "N/A", "NA"]
    return df.filter(
        (col("date").isNotNull())
        & (col("mmm_yy").isNotNull())
        & (col("week_no").isNotNull())
        & (~col("date").isin(*unwanted_values))
        & (~col("mmm_yy").isin(*unwanted_values))
        & (~col("week_no").isin(*unwanted_values))
    )


def format_date_column(df: DataFrame) -> DataFrame:
    """Format the 'date' column to 'dd-MM-yyyy' format."""
    return df.withColumn(
        "date",
        when(
            to_date(col("date"), "dd-MMM-yy").isNotNull(),
            date_format(
                to_date(
                    concat_ws(
                        "-",
                        lit("2024"),
                        date_format(to_date(col("date"), "dd-MMM-yy"), "MMM-dd"),
                    ),
                    "yyyy-MMM-dd",
                ),
                "dd-MM-yyyy",
            ),
        ).otherwise(lit("01-01-2024")),
    )


def format_mmm_yy_column(df: DataFrame) -> DataFrame:
    """Format the 'mmm_yy' column to 'dd-MM-yyyy' format."""
    return df.withColumn(
        "mmm_yy",
        when(
            to_date(col("mmm_yy"), "dd-MMM-yy").isNotNull(),
            date_format(
                to_date(
                    concat_ws(
                        "-",
                        lit("2024"),
                        date_format(to_date(col("mmm_yy"), "dd-MMM-yy"), "MMM-dd"),
                    ),
                    "yyyy-MMM-dd",
                ),
                "dd-MM-yyyy",
            ),
        ).otherwise(lit("01-01-2024")),
    )


def clean_week_no_column(df: DataFrame) -> DataFrame:
    """Clean the 'week_no' column by removing 'W' and casting to integer."""
    return df.withColumn("week_no", regexp_replace(col("week_no"), "W", "")).withColumn(
        "week_no", col("week_no").cast("int")
    )


def drop_invalid_rows(df: DataFrame) -> DataFrame:
    """Drop rows with null 'week_no' values."""
    return df.dropna(subset=["week_no"])


def clean_dates_data(df: DataFrame) -> DataFrame:
    """Clean and transform dates data."""
    df = rename_and_lowercase_columns(df)
    df = clean_special_characters(df)
    df = filter_unwanted_values(df)
    df = format_date_column(df)
    df = format_mmm_yy_column(df)
    df = clean_week_no_column(df)
    df = drop_invalid_rows(df)
    return df


def write_transformed_data(df: DataFrame, s3_output_path: str) -> None:
    """Write the transformed data to an S3 bucket as a single CSV file."""
    # Coalesce the DataFrame into a single partition to ensure one output file
    df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
        s3_output_path
    )


if __name__ == "__main__":
    # Initialize Spark session and Glue context
    spark = SparkSession.builder.appName("DatesDataProcessing").getOrCreate()
    glue_context = GlueContext(spark.sparkContext)
    job = Job(glue_context)
    job.init("dates-data-processing-job")

    # S3 paths
    s3_input_path = "s3://nexabrand-prod-source/data/dates.csv"  # Input file path
    s3_output_folder = "s3://nexabrand-prod-target/dates/"  # Output folder
    s3_temp_output_path = f"{s3_output_folder}temp/"  # Temporary output path

    # Load and clean data
    dates_df = load_dates_data(glue_context, s3_input_path)
    cleaned_dates = clean_dates_data(dates_df)

    # Save the cleaned data to S3 as a single CSV file in a temporary folder
    write_transformed_data(cleaned_dates, s3_temp_output_path)

    # Use boto3 to rename the file to `dates.csv`
    s3_client = boto3.client("s3")
    bucket_name = "nexabrand-prod-target"  # Output bucket name

    # Find the generated CSV file in the temporary folder
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="dates/temp/")
    if "Contents" in response:
        for obj in response["Contents"]:
            if obj["Key"].endswith(".csv"):
                source_key = obj["Key"]
                # Construct the destination key
                destination_key = "dates/dates.csv"
                # Copy the file to the new location
                copy_source = {"Bucket": bucket_name, "Key": source_key}
                s3_client.copy_object(
                    CopySource=copy_source, Bucket=bucket_name, Key=destination_key
                )
                # Delete the original file
                s3_client.delete_object(Bucket=bucket_name, Key=source_key)

    # Delete the temporary folder
    s3_client.delete_object(Bucket=bucket_name, Key="dates/temp/")

    # Commit the Glue job
    job.commit()
