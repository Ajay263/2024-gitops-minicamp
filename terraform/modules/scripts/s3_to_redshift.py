import sys
import boto3
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict
import pyarrow.parquet as pq
import io
import psycopg2
from psycopg2 import sql

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment configuration
ENVIRONMENT = 'prod'  # Based on your terraform config

# Get job parameters
args = {
    'source-bucket': f'nexabrands-{ENVIRONMENT}-source',  # Matches your S3 bucket naming
    'target-bucket': f'nexabrands-{ENVIRONMENT}-target',
    'redshift-database': 'nexabrands_datawarehouse',  # From your Redshift config
    'redshift-schema': 'public',  # Default schema, adjust if needed
    'redshift-workgroup': 'nexabrands-redshift-workgroup',
    'redshift-namespace': 'nexabrands-redshift-namespace',
    'redshift-username': 'admin',
    'redshift-password': 'Password123!'  # In production, use AWS Secrets Manager
}

# Initialize AWS clients
s3 = boto3.client('s3', region_name='us-east-1')
redshift_serverless = boto3.client('redshift-serverless', region_name='us-east-1')

class S3ToRedshiftETL:
    def __init__(self):
        self.source_bucket = args['source-bucket']
        self.target_bucket = args['target-bucket']
        self.database = args['redshift-database']
        self.schema = args['redshift-schema']
        self.workgroup = args['redshift-workgroup']
        self.namespace = args['redshift-namespace']
        self.batch_size = 100000
        
        # Get Redshift endpoint
        try:
            workgroup_response = redshift_serverless.get_workgroup(
                workgroupName=self.workgroup
            )
            self.redshift_endpoint = workgroup_response['workgroup']['endpoint']['address']
            
            # Construct Redshift connection string
            self.redshift_conn_string = (
                f"host={self.redshift_endpoint} "
                f"dbname={self.database} "
                f"user={args['redshift-username']} "
                f"password={args['redshift-password']} "
                f"port=5439"
            )
        except Exception as e:
            logger.error(f"Error getting Redshift endpoint: {str(e)}")
            raise
    
    def list_parquet_files(self, prefix: str) -> List[str]:
        """List all parquet files in the given S3 prefix."""
        try:
            paginator = s3.get_paginator('list_objects_v2')
            files = []
            
            for page in paginator.paginate(Bucket=self.source_bucket, Prefix=prefix):
                if 'Contents' in page:
                    files.extend([
                        obj['Key']
                        for obj in page['Contents']
                        if obj['Key'].endswith('.parquet')
                    ])
            
            return files
        except Exception as e:
            logger.error(f"Error listing files in {prefix}: {str(e)}")
            raise

    def read_parquet_from_s3(self, file_key: str) -> pd.DataFrame:
        """Read a parquet file from S3 into a pandas DataFrame."""
        try:
            response = s3.get_object(Bucket=self.source_bucket, Key=file_key)
            parquet_buffer = io.BytesIO(response['Body'].read())
            return pd.read_parquet(parquet_buffer)
        except Exception as e:
            logger.error(f"Error reading parquet file {file_key}: {str(e)}")
            raise

    def create_table_if_not_exists(self, df: pd.DataFrame, table_name: str):
        """Create table in Redshift if it doesn't exist based on DataFrame schema."""
        type_mapping = {
            'int64': 'BIGINT',
            'float64': 'DOUBLE PRECISION',
            'object': 'VARCHAR(MAX)',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP'
        }

        columns = []
        for col_name, dtype in df.dtypes.items():
            sql_type = type_mapping.get(str(dtype), 'VARCHAR(MAX)')
            columns.append(f'"{col_name}" {sql_type}')

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.schema}.{table_name} (
            {', '.join(columns)}
        );
        """

        with psycopg2.connect(self.redshift_conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_sql)
            conn.commit()

    def copy_to_redshift(self, df: pd.DataFrame, table_name: str):
        """Copy DataFrame to Redshift using COPY command."""
        # Create temporary CSV in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)
        
        with psycopg2.connect(self.redshift_conn_string) as conn:
            with conn.cursor() as cur:
                # Use COPY command to load data
                copy_sql = f"""
                COPY {self.schema}.{table_name}
                FROM STDIN WITH CSV
                """
                cur.copy_expert(sql=copy_sql, file=csv_buffer)
            conn.commit()

    def process_table(self, source_prefix: str, table_name: str) -> None:
        """Process a single table's data from S3 to Redshift."""
        try:
            logger.info(f"Starting processing for table: {table_name}")
            
            # List all parquet files for this table
            parquet_files = self.list_parquet_files(source_prefix)
            
            if not parquet_files:
                logger.warning(f"No parquet files found in {source_prefix}")
                return
                
            # Create table if it doesn't exist using first file
            first_df = self.read_parquet_from_s3(parquet_files[0])
            self.create_table_if_not_exists(first_df, table_name)
            
            # Process files in batches
            for parquet_file in parquet_files:
                logger.info(f"Processing file: {parquet_file}")
                
                df = self.read_parquet_from_s3(parquet_file)
                
                # Process in chunks to optimize memory usage
                for i in range(0, len(df), self.batch_size):
                    chunk = df[i:i + self.batch_size]
                    self.copy_to_redshift(chunk, table_name)
                    
                logger.info(f"Completed processing file: {parquet_file}")
                
            logger.info(f"Completed processing table: {table_name}")
            
        except Exception as e:
            logger.error(f"Error processing table {table_name}: {str(e)}")
            raise

    def run(self):
        """Main ETL process."""
        try:
            # Define table configurations
            tables = {
                'customers': 'processed_customers/',
                'dates': 'processed_dates/',
                'products': 'processed_products/'
            }
            
            # Process tables in parallel
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = []
                for table_name, prefix in tables.items():
                    futures.append(
                        executor.submit(self.process_table, prefix, table_name)
                    )
                
                # Wait for all tasks to complete
                for future in futures:
                    future.result()
                    
            logger.info("ETL process completed successfully")
            
        except Exception as e:
            logger.error(f"ETL process failed: {str(e)}")
            raise

def main():
    etl = S3ToRedshiftETL()
    etl.run()

if __name__ == '__main__':
    main()