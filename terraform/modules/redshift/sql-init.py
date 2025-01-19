import boto3
import sys
import time

def check_object_exists(redshift_client, database_name, workgroup_name, query):
    """
    Check if an object exists in Redshift using the provided query
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
    Execute SQL statements in Redshift Serverless using the ExecuteStatement API
    """
    redshift_client = boto3.client('redshift-data')
    
    results = []
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
                    print(f"Warning: {error_message} - continuing execution")
                    continue
                raise Exception(f"Query failed: {error_message}\nSQL: {sql}")
            
            results.append({
                'sql': sql,
                'status': status['Status']
            })
            
        except Exception as e:
            if 'already exists' in str(e).lower():
                print(f"Warning: {str(e)} - continuing execution")
                continue
            print(f"Error executing SQL: {str(e)}")
            sys.exit(1)
    
    return results

def main():
    if len(sys.argv) != 7:
        print("Usage: script.py <database_name> <workgroup_name> <iam_role_arn> <dbt_password> <account_id> <glue_database>")
        sys.exit(1)
    
    database_name = sys.argv[1]
    workgroup_name = sys.argv[2]
    iam_role_arn = sys.argv[3]
    dbt_password = sys.argv[4]
    account_id = sys.argv[5]
    glue_database = sys.argv[6]
    
    redshift_client = boto3.client('redshift-data')
    
    # Check if schemas exist
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
    
    # Check if user exists
    user_exists = check_object_exists(
        redshift_client,
        database_name,
        workgroup_name,
        "SELECT 1 FROM pg_user WHERE usename = 'dbt'"
    )
    
    # Check if group exists
    group_exists = check_object_exists(
        redshift_client,
        database_name,
        workgroup_name,
        "SELECT 1 FROM pg_group WHERE groname = 'dbt'"
    )
    
    sql_statements = []
    
    # Schema creation statements
    if not external_schema_exists:
        sql_statements.append(f"""
            create external schema tickit_external
            from data catalog
            database '{glue_database}'
            iam_role '{iam_role_arn}'
            create external database if not exists;
        """)
    
    if not dbt_schema_exists:
        sql_statements.append("create schema tickit_dbt;")
    
    # Drop public schema if it exists
    sql_statements.append("drop schema if exists public cascade;")
    
    # User creation/alteration
    if not user_exists:
        sql_statements.append(
            f"create user dbt with password '{dbt_password}' nocreatedb nocreateuser syslog access restricted connection limit 10;"
        )
    else:
        sql_statements.append(
            f"alter user dbt with password '{dbt_password}' nocreatedb nocreateuser syslog access restricted connection limit 10;"
        )
    
    # Group creation and user assignment
    if not group_exists:
        sql_statements.append("create group dbt;")
    
    # Add user to group (this is idempotent in Redshift)
    sql_statements.append("alter group dbt add user dbt;")
    
    # Grants
    sql_statements.extend([
        # Grants on tickit_external schema
        "grant usage on schema tickit_external to group dbt;",
        "grant create on schema tickit_external to group dbt;",
        "grant all on all tables in schema tickit_external to group dbt;",
        
        # Grants on tickit_dbt schema
        "grant usage on schema tickit_dbt to group dbt;",
        "grant create on schema tickit_dbt to group dbt;",
        "grant all on all tables in schema tickit_dbt to group dbt;",
        
        # Reassign schema ownership
        "alter schema tickit_dbt owner to dbt;",
        "alter schema tickit_external owner to dbt;"
    ])
    
    try:
        results = execute_sql(sql_statements, database_name, workgroup_name)
        print("SQL initialization completed successfully")
    except Exception as e:
        print(f"Error during SQL initialization: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()