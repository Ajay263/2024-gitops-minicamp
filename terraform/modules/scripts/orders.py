from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import (
    col,
    regexp_replace,
    to_date,
    trim,
    upper,
    when,
    coalesce,
    sum as sum_,
)
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def load_orders_data(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Load orders data from a CSV file.
    """
    schema = StructType(
        [
            StructField("ORDER_ID", StringType(), True),
            StructField("customer_id", StringType(), True),  # Updated column name
            StructField("order_placement_date", StringType(), True),
        ]
    )
    df = spark.read.format("csv").option("header", True).schema(schema).load(file_path)
    print(f"Row count after loading: {df.count()}")
    print("Sample of raw data:")
    df.show(5, truncate=False)
    return df


def clean_orders_data(df: DataFrame) -> DataFrame:
    """
    Clean and transform orders data.
    """
    orders_df = df.selectExpr(
        "ORDER_ID as order_id",
        "customer_id", 
        "order_placement_date",
    )

 
    unwanted_values = ["NA", "none", "NULL", "N/A"]
    for column in orders_df.columns:
        orders_df = orders_df.filter(~trim(col(column)).isin(unwanted_values))
    
    print(f"Row count after unwanted values filtering: {orders_df.count()}")

    orders_df = orders_df.withColumn(
        "order_id",
        when(
            (col("order_id").isin("N/A", "Unknown", "null", "None", None))
            | (col("order_id").rlike("[^a-zA-Z0-9]")),
            None,
        ).otherwise(upper(trim(col("order_id")))),
    )
    
    print(f"Row count after order_id cleaning: {orders_df.count()}")

    orders_df = orders_df.withColumn(
        "customer_id",
        when(
            (col("customer_id").isNull())
            | (col("customer_id").isin("Unknown", "null", "None", None))
            | (col("customer_id").like("ID_%"))
            | (col("customer_id").rlike("[^0-9.]")),
            None,
        ).otherwise(col("customer_id").cast(IntegerType())),
    )
    
    print(f"Row count after customer_id cleaning: {orders_df.count()}")

    # More flexible date parsing with multiple format options
    orders_df = orders_df.withColumn(
        "order_placement_date",
        coalesce(
            to_date(col("order_placement_date"), "MM/dd/yyyy"),
            to_date(col("order_placement_date"), "yyyy-MM-dd"),
            to_date(col("order_placement_date"), "dd-MMM-yyyy"),
            to_date(regexp_replace(col("order_placement_date"), "(?i)([a-z]+,\\s)|\\s+", ""), "MM/dd/yyyy")
        )
    )
    
    print(f"Row count after date cleaning: {orders_df.count()}")
    

    print("Sample data before final filtering:")
    orders_df.show(5, truncate=False)

    null_counts = orders_df.select([
        sum_(col(c).isNull().cast("int")).alias(c) 
        for c in orders_df.columns
    ])
    print("Count of null values in each column:")
    null_counts.show()

    # Temporary version without dropna to check
    clean_with_nulls = orders_df.dropDuplicates()
    print(f"Row count after only dropDuplicates (keeping nulls): {clean_with_nulls.count()}")
    
    # Now apply the full cleaning
    final_df = orders_df.dropna().dropDuplicates()
    print(f"Row count after full cleaning (dropna + dropDuplicates): {final_df.count()}")
    

    if final_df.count() == 0:
        print("WARNING: All rows were filtered out by dropna(). Returning version with nulls.")
        return clean_with_nulls
    else:
        return final_df


if __name__ == "__main__":
    spark = SparkSession.builder.appName("OrdersDataProcessing").getOrCreate()


    input_path = "s3a://nexabrand-prod-source/data/orders.csv"
    output_path = "s3a://nexabrand-prod-target/orders/orders.csv"

    orders_df = load_orders_data(spark, input_path)
    cleaned_orders = clean_orders_data(orders_df)

    print("Final data to be written:")
    cleaned_orders.show(5, truncate=False)
    print(f"Final row count: {cleaned_orders.count()}")

    cleaned_orders.write.mode("overwrite").option("header", "true").option(
        "quote", '"'
    ).option("escape", '"').csv(output_path)

    print(f"Data written to {output_path}")
    spark.stop()
