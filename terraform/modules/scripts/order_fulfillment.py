# AWS
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import (
    col,
    lit,
    regexp_replace,
    trim,
    upper,
    when,
)
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def load_order_fulfillment_data(
    glue_context: GlueContext, s3_input_path: str
) -> DataFrame:
    """Load order fulfillment data from a CSV file in S3 using GlueContext."""
    schema = StructType(
        [
            StructField("ORDER_ID", StringType(), True),
            StructField("ON.TIME", FloatType(), True),
            StructField("IN_FULL", FloatType(), True),
            StructField("OTIF", FloatType(), True),
        ]
    )
    return (
        glue_context.spark_session.read.format("csv")
        .option("header", True)
        .schema(schema)
        .load(s3_input_path)
    )


def rename_columns(df: DataFrame) -> DataFrame:
    """Rename columns to lowercase and replace special characters."""
    df = df.withColumnRenamed("ON.TIME", "on_time")
    return df.select([col(c).alias(c.lower()) for c in df.columns])


def clean_order_id(df: DataFrame) -> DataFrame:
    """Clean the 'order_id' column by trimming and removing invalid characters."""
    return df.withColumn(
        "order_id",
        upper(regexp_replace(trim(col("order_id")), r"[^a-zA-Z0-9]", "")),
    )


def filter_invalid_order_ids(df: DataFrame) -> DataFrame:
    """Filter out rows with invalid 'order_id' values."""
    invalid_values = ["n/a", "N/A", "NONE", "NULL", "null", "unknown", "NA"]
    return df.filter(~col("order_id").cast("string").isin(invalid_values))


def transform_metrics(df: DataFrame) -> DataFrame:
    """Transform the metrics columns ('on_time', 'in_full', 'otif')."""
    for column in ["on_time", "in_full", "otif"]:
        df = df.withColumn(
            column,
            when(col(column) == -1, 1)
            .when(col(column) == 1, 1)
            .when(col(column) <= 0.5, 0)
            .when(col(column) > 1, lit(None))  
            .otherwise(1)  
            .cast(IntegerType()),
        )
    return df


def drop_null_values(df: DataFrame) -> DataFrame:
    """Drop rows with null values."""
    return df.dropna()


def clean_order_fulfillment_data(df: DataFrame) -> DataFrame:
    """Clean and transform order fulfillment data."""
    df = rename_columns(df)
    df = drop_null_values(df)
    df = clean_order_id(df)
    df = filter_invalid_order_ids(df)
    df = transform_metrics(df)
    return df


def write_transformed_data(df: DataFrame, s3_output_path: str) -> None:
    """Write the transformed data to an S3 bucket as a single CSV file."""
    df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
        s3_output_path
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("OrderFulfillmentDataProcessing").getOrCreate()
    glue_context = GlueContext(spark.sparkContext)
    job = Job(glue_context)
    job.init("order-fulfillment-job")

    s3_input_path = (
        "s3://nexabrand-prod-source/data/order_fulfillment.csv"  
    )
    s3_output_folder = "s3://nexabrand-prod-target/order_fulfillment/"  
    s3_temp_output_path = f"{s3_output_folder}temp/"  

    order_fulfillment_df = load_order_fulfillment_data(glue_context, s3_input_path)
    cleaned_order_fulfillment = clean_order_fulfillment_data(order_fulfillment_df)

    write_transformed_data(cleaned_order_fulfillment, s3_temp_output_path)

    import boto3

    s3_client = boto3.client("s3")
    bucket_name = "nexabrand-prod-target"  

    response = s3_client.list_objects_v2(
        Bucket=bucket_name, Prefix="order_fulfillment/temp/"
    )
    if "Contents" in response:
        for obj in response["Contents"]:
            if obj["Key"].endswith(".csv"):
                source_key = obj["Key"]
                destination_key = "order_fulfillment/order_fulfillment.csv"
                copy_source = {"Bucket": bucket_name, "Key": source_key}
                s3_client.copy_object(
                    CopySource=copy_source, Bucket=bucket_name, Key=destination_key
                )
                s3_client.delete_object(Bucket=bucket_name, Key=source_key)

    s3_client.delete_object(Bucket=bucket_name, Key="order_fulfillment/temp/")

    job.commit()
