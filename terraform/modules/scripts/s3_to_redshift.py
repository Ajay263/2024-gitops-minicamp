import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging
from typing import Dict

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
        'source_bucket',
        'redshift_connection',
        'redshift_database',
        'redshift_schema',
        'redshift_temp_dir'
    ])
    return args

def read_source_data(glueContext: GlueContext, source_path: str, format: str, logger: logging.Logger, format_options=None):
    """Read data from S3 using Glue Dynamic Frame"""
    logger.info(f"Reading {format} data from: {source_path}")
    
    try:
        connection_options = {
            "paths": [source_path]
        }
        if format_options:
            connection_options.update(format_options)
            
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options=connection_options,
            format=format,
            transformation_ctx=f"source_data_{format}"
        )
        
        return dynamic_frame
    except Exception as e:
        logger.error(f"Error reading from {source_path}: {str(e)}")
        raise

def write_to_redshift(
    glueContext: GlueContext,
    dynamic_frame,
    connection_name: str,
    table_name: str,
    database: str,
    redshift_tmp_dir: str,
    transformation_ctx: str,
    logger: logging.Logger
):
    """Write Dynamic Frame to Redshift"""
    logger.info(f"Writing data to Redshift table: {table_name}")
    
    try:
        sink_dyf = glueContext.write_dynamic_frame.from_jdbc_conf(
            frame=dynamic_frame,
            catalog_connection=connection_name,
            connection_options={
                "dbtable": f"{table_name}",
                "database": database
            },
            redshift_tmp_dir=redshift_tmp_dir,
            transformation_ctx=transformation_ctx
        )
        logger.info(f"Successfully wrote data to {table_name}")
        return sink_dyf
    except Exception as e:
        logger.error(f"Error writing to Redshift table {table_name}: {str(e)}")
        raise

def apply_mappings(dynamic_frame, mapping_list, transformation_ctx: str):
    """Apply column mappings to dynamic frame"""
    return ApplyMapping.apply(
        frame=dynamic_frame,
        mappings=mapping_list,
        transformation_ctx=transformation_ctx
    )

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
    
    # Configure table mappings and their data types
    table_configs = {
        'customers': {
            'source_path': f"s3://{args['source_bucket']}/customers/",
            'format': 'parquet',
            'mappings': [
                ("customer_id", "string", "customer_id", "varchar"),
                ("customer_name", "string", "customer_name", "varchar"),
                ("city", "string", "city", "varchar")
            ]
        },
        'products': {
            'source_path': f"s3://{args['source_bucket']}/products/",
            'format': 'parquet',
            'mappings': [
                ("product_id", "string", "product_id", "varchar"),
                ("product_name", "string", "product_name", "varchar"),
                ("category", "string", "category", "varchar")
            ]
        }
    }
    
    # Process each table
    for table_name, config in table_configs.items():
        try:
            logger.info(f"Processing table: {table_name}")
            
            # Read source data
            source_dyf = read_source_data(
                glueContext,
                config['source_path'],
                config['format'],
                logger
            )
            
            # Apply mappings
            mapped_dyf = apply_mappings(
                source_dyf,
                config['mappings'],
                f"apply_mapping_{table_name}"
            )
            
            # Write to Redshift
            write_to_redshift(
                glueContext=glueContext,
                dynamic_frame=mapped_dyf,
                connection_name=args['redshift_connection'],
                table_name=f"{args['redshift_schema']}.{table_name}",
                database=args['redshift_database'],
                redshift_tmp_dir=args['redshift_temp_dir'],
                transformation_ctx=f"write_{table_name}",
                logger=logger
            )
            
        except Exception as e:
            logger.error(f"Failed to process table {table_name}: {str(e)}")
            raise
    
    logger.info("ETL job completed successfully")
    job.commit()

if __name__ == "__main__":
    main()