import boto3
import sys
import time

def check_object_exists(redshift_client, database_name, workgroup_name, query):
    """
    Check if an object exists in Redshift
    """
    try:
        response = redshift_client.execute_statement(
            Database=database_name,
            WorkgroupName=workgroup_name,
            Sql=query
        )
        
        query_id = response['Id']
        while True:
            status = redshift_client.describe_statement(Id=query_id)
            if status['Status'] in ['FINISHED', 'FAILED', 'ABORTED']:
                break
            time.sleep(0.5)
            
        if status['Status'] == 'FINISHED':
            result = redshift_client.get_statement_result(Id=query_id)
            return len(result.get('Records', [])) > 0
            
        return False
    except Exception:
        return False

def execute_sql(sql_statements, database_name, workgroup_name):
    """
    Execute SQL statements in Redshift Serverless
    """
    redshift_client = boto3.client('redshift-data')
    
    for sql in sql_statements:
        if not sql.strip():
            continue
            
        try:
            response = redshift_client.execute_statement(
                Database=database_name,
                WorkgroupName=workgroup_name,
                Sql=sql.strip()
            )
            
            query_id = response['Id']
            while True:
                status = redshift_client.describe_statement(Id=query_id)
                if status['Status'] in ['FINISHED', 'FAILED', 'ABORTED']:
                    break
                time.sleep(0.5)
            
            if status['Status'] == 'FAILED':
                error_message = status.get('Error', 'Unknown error')
                if 'already exists' in error_message.lower():
                    continue
                raise Exception(f"Query failed: {error_message}\nSQL: {sql}")
                
        except Exception as e:
            if 'already exists' in str(e).lower():
                continue
            print(f"Error executing SQL: {str(e)}")
            sys.exit(1)

def main():
    if len(sys.argv) != 5:
        print("Usage: script.py <database_name> <workgroup_name> <iam_role_arn> <dbt_password>")
        sys.exit(1)
    
    database_name = sys.argv[1]
    workgroup_name = sys.argv[2]
    iam_role_arn = sys.argv[3]
    dbt_password = sys.argv[4]
    
    redshift_client = boto3.client('redshift-data')
    
    # Check what exists
    external_schema_exists = check_object_exists(
        redshift_client, 
        database_name, 
        workgroup_name, 
        "SELECT 1 FROM pg_namespace WHERE nspname = 'tickit_external'"
    )
    
    dbt_schema_exists = check_object_exists(
        redshift_client, 
        database_name, 
        workgroup_name, 
        "SELECT 1 FROM pg_namespace WHERE nspname = 'tickit_dbt'"
    )
    
    public_schema_exists = check_object_exists(
        redshift_client, 
        database_name, 
        workgroup_name, 
        "SELECT 1 FROM pg_namespace WHERE nspname = 'public'"
    )
    
    user_exists = check_object_exists(
        redshift_client,
        database_name,
        workgroup_name,
        "SELECT 1 FROM pg_user WHERE usename = 'dbt'"
    )
    
    # Build SQL statements based on what exists
    sql_statements = []
    
    # Only create schemas if they don't exist
    if not external_schema_exists:
        sql_statements.append(f"""
            CREATE EXTERNAL SCHEMA tickit_external
            FROM DATA CATALOG
            DATABASE 'tickit_dbt'
            IAM_ROLE '{iam_role_arn}'
            CREATE EXTERNAL DATABASE IF NOT EXISTS;
        """)
    
    if not dbt_schema_exists:
        sql_statements.append("CREATE SCHEMA tickit_dbt;")
    
    if public_schema_exists:
        sql_statements.append("DROP SCHEMA public CASCADE;")
    
    if not user_exists:
        sql_statements.extend([
            f"""
            CREATE USER dbt WITH PASSWORD '{dbt_password}'
            NOCREATEDB NOCREATEUSER SYSLOG ACCESS RESTRICTED
            CONNECTION LIMIT 10;
            """,
            "CREATE GROUP dbt;",
            "ALTER GROUP dbt ADD USER dbt;"
        ])
    
    # Always apply grants (these are idempotent)
    sql_statements.extend([
        # Grants on tickit_external schema
        "GRANT USAGE ON SCHEMA tickit_external TO GROUP dbt;",
        "GRANT CREATE ON SCHEMA tickit_external TO GROUP dbt;",
        "GRANT ALL ON ALL TABLES IN SCHEMA tickit_external TO GROUP dbt;",
        
        # Grants on tickit_dbt schema
        "GRANT USAGE ON SCHEMA tickit_dbt TO GROUP dbt;",
        "GRANT CREATE ON SCHEMA tickit_dbt TO GROUP dbt;",
        "GRANT ALL ON ALL TABLES IN SCHEMA tickit_dbt TO GROUP dbt;",
        
        # Reassign schema ownership
        "ALTER SCHEMA tickit_dbt OWNER TO dbt;",
        "ALTER SCHEMA tickit_external OWNER TO dbt;"
    ])
    
    try:
        execute_sql(sql_statements, database_name, workgroup_name)
        print("Setup completed successfully!")
    except Exception as e:
        print(f"Setup failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()