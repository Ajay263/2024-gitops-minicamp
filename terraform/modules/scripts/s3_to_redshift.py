import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
import logging
from typing import Dict
import pg8000

def setup_logging():
    """Configure logging for the ETL job"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger("S3ToRedshiftETL")

def get_job_arguments() -> Dict:
    """Get job arguments passed from Glue job"""
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'source-bucket',
        'redshift-database',
        'redshift-schema',
        'redshift-workgroup',
        'redshift-temp-dir'
    ])
    return args

def create_redshift_tables(redshift_properties: Dict, logger: logging.Logger):
    """Create Redshift tables if they don't exist"""
    create_table_statements = {
        'processed_customers': """
            CREATE TABLE IF NOT EXISTS {schema}.processed_customers (
                customer_id VARCHAR(32) PRIMARY KEY,
                customer_name VARCHAR(255),
                city VARCHAR(100)
            ) DISTSTYLE KEY DISTKEY(customer_id);
        """,
        'processed_products': """
            CREATE TABLE IF NOT EXISTS {schema}.processed_products (
                product_id VARCHAR(32) PRIMARY KEY,
                product_name VARCHAR(255),
                category VARCHAR(100)
            ) DISTSTYLE KEY DISTKEY(product_id);
        """,
        'processed_dates': """
            CREATE TABLE IF NOT EXISTS {schema}.processed_dates (
                date DATE PRIMARY KEY,
                mmm_yy VARCHAR(10),
                week_no VARCHAR(10)
            ) DISTSTYLE ALL;
        """
    }

    try:
        conn = pg8000.connect(
            database=redshift_properties['database'],
            user="admin",
            password="Password123!",
            host=f"{redshift_properties['workgroup']}.{redshift_properties['region']}.redshift-serverless.amazonaws.com"
        )
        
        cursor = conn.cursor()
        
        # Create schema if it doesn't exist
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {redshift_properties['schema']};")
        
        # Create tables if they don't exist
        for table_name, create_statement in create_table_statements.items():
            formatted_statement = create_statement.format(schema=redshift_properties['schema'])
            cursor.execute(formatted_statement)
            logger.info(f"Created or verified table: {table_name}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error creating Redshift tables: {str(e)}")
        raise

def read_parquet_data(glueContext: GlueContext, s3_path: str, logger: logging.Logger):
    """Read parquet data from S3"""
    logger.info(f"Reading parquet data from: {s3_path}")
    try:
        # Read using spark session directly
        spark = glueContext.spark_session
        df = spark.read.parquet(s3_path)
        count = df.count()
        logger.info(f"Successfully read {count} records from {s3_path}")
        return df
    except Exception as e:
        logger.error(f"Error reading from {s3_path}: {str(e)}")
        raise

def write_to_redshift(
    df,
    redshift_table: str,
    redshift_properties: Dict,
    temp_s3_dir: str,
    logger: logging.Logger
):
    """Write DataFrame to Redshift"""
    logger.info(f"Writing data to Redshift table: {redshift_table}")
    
    try:
        # Using spark native JDBC write
        df.write \
            .format("jdbc") \
            .option("url", f"jdbc:redshift:iam://{redshift_properties['workgroup']}/{redshift_properties['database']}") \
            .option("dbtable", f"{redshift_properties['schema']}.{redshift_table}") \
            .option("tempdir", temp_s3_dir) \
            .option("aws_iam_role", "auto") \
            .mode("append") \
            .save()
        
        logger.info(f"Successfully wrote data to {redshift_table}")
    except Exception as e:
        logger.error(f"Error writing to Redshift table {redshift_table}: {str(e)}")
        raise

def main():
    # Initialize Glue context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    logger = setup_logging()
    
    # Get job arguments
    args = get_job_arguments()
    job.init(args['JOB_NAME'])
    
    # Configure Redshift properties
    redshift_properties = {
        'database': args['redshift-database'],
        'schema': args['redshift-schema'],
        'workgroup': args['redshift-workgroup'],
        'region': 'us-east-1'
    }
    
    # Create Redshift tables if they don't exist
    create_redshift_tables(redshift_properties, logger)
    
    # Define S3 paths
    source_bucket = args['source-bucket']
    temp_dir = args['redshift-temp-dir']
    
    # Define table mappings
    table_mappings = {
        'processed_customers': f"s3://{source_bucket}/processed_customers/",
        'processed_dates': f"s3://{source_bucket}/processed_dates/",
        'processed_products': f"s3://{source_bucket}/processed_products/"
    }
    
    # Process each table
    for table_name, s3_path in table_mappings.items():
        try:
            logger.info(f"Processing table: {table_name}")
            
            # Read parquet data
            df = read_parquet_data(glueContext, s3_path, logger)
            
            # Write to Redshift
            write_to_redshift(
                df=df,
                redshift_table=table_name,
                redshift_properties=redshift_properties,
                temp_s3_dir=temp_dir,
                logger=logger
            )
            
        except Exception as e:
            logger.error(f"Failed to process table {table_name}: {str(e)}")
            raise
    
    logger.info("ETL job completed successfully")
    job.commit()

if __name__ == "__main__":
    main()