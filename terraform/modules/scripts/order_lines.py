import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import (
    col,
    concat,
    datediff,
    lit,
    lpad,
    regexp_replace,
    regexp_extract,
    round,
    to_date,
    trim,
    upper,
    when,
    year,
    month,
    dayofmonth,
)
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def load_order_lines_data(glue_context: GlueContext, s3_input_path: str) -> DataFrame:
    """Load order lines data from a CSV file in S3 using GlueContext."""
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
    return (
        glue_context.spark_session.read.format("csv")
        .option("header", True)
        .schema(schema)
        .load(s3_input_path)
    )


def clean_order_id_and_product_id(df: DataFrame) -> DataFrame:
    """Clean ORDER_ID and PRODUCT_ID columns."""
    return (
        df.withColumn(
            "ORDER_ID",
            upper(regexp_replace(trim(col("ORDER_ID")), r"[^a-zA-Z0-9]", "")),
        )
        .withColumn("PRODUCT_ID", regexp_replace(col("PRODUCT_ID"), r"[^0-9]", ""))
        .withColumn("PRODUCT_ID", col("PRODUCT_ID").cast(IntegerType()))
    )


def clean_order_qty_and_delivery_qty(df: DataFrame) -> DataFrame:
    """Clean ORDER_QTY and DELIVERY_QTY columns."""
    return (
        df.withColumn("ORDER_QTY", col("ORDER_QTY").cast(IntegerType()))
        .withColumn("DELIVERY_QTY", regexp_replace(col("DELIVERY_QTY"), r"[^0-9]", ""))
        .withColumn("DELIVERY_QTY", col("DELIVERY_QTY").cast(IntegerType()))
    )


def filter_invalid_quantities(df: DataFrame) -> DataFrame:
    """Filter out rows with invalid quantities."""
    return df.filter((col("ORDER_QTY") > 0) & (col("DELIVERY_QTY") > 0))


def clean_agreed_delivery_date(df: DataFrame) -> DataFrame:
    """Clean and parse AGREED_DELIVERY_DATE column, setting all years to 2024."""
    return (
        df.withColumn(
            "AGREED_DELIVERY_DATE",
            # Extract the month day, year part
            regexp_extract(col("AGREED_DELIVERY_DATE"), r"([A-Za-z]+, [A-Za-z]+ \d+, \d{4})", 1)
        )
        .withColumn(
            "AGREED_DELIVERY_DATE",
            to_date(col("AGREED_DELIVERY_DATE"), "EEEE, MMMM d, yyyy")
        )
        # Extract month and day, then reconstruct with year 2024
        .withColumn(
            "AGREED_DELIVERY_DATE",
            to_date(
                concat(
                    lit("2024-"),
                    lpad(month(col("AGREED_DELIVERY_DATE")).cast("string"), 2, "0"),
                    lit("-"),
                    lpad(dayofmonth(col("AGREED_DELIVERY_DATE")).cast("string"), 2, "0")
                ),
                "yyyy-MM-dd"
            )
        )
    )


def clean_actual_delivery_date(df: DataFrame) -> DataFrame:
    """Clean and parse ACTUAL_DELIVERY_DATE column, setting all years to 2024."""
    return (
        df.withColumn(
            "ACTUAL_DELIVERY_DATE",
            # Extract the month day, year part
            regexp_extract(col("ACTUAL_DELIVERY_DATE"), r"([A-Za-z]+, [A-Za-z]+ \d+, \d{4})", 1)
        )
        .withColumn(
            "ACTUAL_DELIVERY_DATE",
            to_date(col("ACTUAL_DELIVERY_DATE"), "EEEE, MMMM d, yyyy")
        )
        # Extract month and day, then reconstruct with year 2024
        .withColumn(
            "ACTUAL_DELIVERY_DATE",
            to_date(
                concat(
                    lit("2024-"),
                    lpad(month(col("ACTUAL_DELIVERY_DATE")).cast("string"), 2, "0"),
                    lit("-"),
                    lpad(dayofmonth(col("ACTUAL_DELIVERY_DATE")).cast("string"), 2, "0")
                ),
                "yyyy-MM-dd"
            )
        )
    )


def filter_unwanted_values(df: DataFrame, unwanted_values: list) -> DataFrame:
    """Filter out rows with unwanted values in any column."""
    for column in df.columns:
        df = df.filter(~trim(col(column)).isin(unwanted_values))
    return df


def drop_null_values(df: DataFrame) -> DataFrame:
    """Drop rows with null values in any column."""
    return df.dropna()


def convert_column_names_to_lowercase(df: DataFrame) -> DataFrame:
    """Convert column names to lowercase."""
    return df.select([col(c).alias(c.lower()) for c in df.columns])


def add_derived_columns(df: DataFrame) -> DataFrame:
    """Add derived columns for analysis."""
    return (
        df.withColumn(
            "delivery_delay_days",
            datediff(col("actual_delivery_date"), col("agreed_delivery_date")),
        )
        .withColumn(
            "delivery_completion_rate",
            round(col("delivery_qty") / col("order_qty") * 100, 2),
        )
        .withColumn(
            "is_on_time",
            when(col("delivery_delay_days") <= 0, "Yes").otherwise("No"),
        )
        .withColumn(
            "is_complete_delivery",
            when(col("delivery_completion_rate") >= 100, "Yes").otherwise("No"),
        )
    )


def clean_order_lines_data(df: DataFrame) -> DataFrame:
    """Clean and transform order lines data."""
    # Define unwanted values
    unwanted_values = ["NULL", "null", "NA", "none", "N/A"]

    # Apply transformations
    df = clean_order_id_and_product_id(df)
    df = clean_order_qty_and_delivery_qty(df)
    df = filter_invalid_quantities(df)
    df = clean_agreed_delivery_date(df)
    df = clean_actual_delivery_date(df)
    df = filter_unwanted_values(df, unwanted_values)
    df = drop_null_values(df)
    df = convert_column_names_to_lowercase(df)
    df = add_derived_columns(df)

    return df


def write_transformed_data(df: DataFrame, s3_output_path: str) -> None:
    """Write the transformed data to an S3 bucket as a single CSV file."""
    df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
        s3_output_path
    )


if _name_ == "_main_":
    # Initialize Spark session and Glue context
    spark = SparkSession.builder \
        .appName("OrderLinesDataProcessing") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    glue_context = GlueContext(spark.sparkContext)
    job = Job(glue_context)
    job.init("order-lines-data-processing-job")

    # S3 paths
    s3_input_path = "s3://nexabrand-prod-source/data/order_lines.csv"  # Input file path
    s3_output_folder = "s3://nexabrand-prod-target/order_lines/"  # Output folder
    s3_temp_output_path = f"{s3_output_folder}temp/"  # Temporary output path

    # Load and clean data
    order_lines_df = load_order_lines_data(glue_context, s3_input_path)
    cleaned_order_lines = clean_order_lines_data(order_lines_df)

    # Save the cleaned data to S3 as a single CSV file in a temporary folder
    write_transformed_data(cleaned_order_lines, s3_temp_output_path)

    # Use boto3 to rename the file to order_lines.csv
    s3_client = boto3.client("s3")
    bucket_name = "nexabrand-prod-target"  # Output bucket name

    # Find the generated CSV file in the temporary folder
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="order_lines/temp/")
    if "Contents" in response:
        for obj in response["Contents"]:
            if obj["Key"].endswith(".csv"):
                source_key = obj["Key"]
                # Construct the destination key
                destination_key = "order_lines/order_lines.csv"
                # Copy the file to the new location
                copy_source = {"Bucket": bucket_name, "Key": source_key}
                s3_client.copy_object(
                    CopySource=copy_source, Bucket=bucket_name, Key=destination_key
                )
                # Delete the original file
                s3_client.delete_object(Bucket=bucket_name, Key=source_key)

    # Delete the temporary folder
    s3_client.delete_object(Bucket=bucket_name, Key="order_lines/temp/")

    # Commit the Glue job
    job.commit()