# modules/redshift/sql-init.py

import boto3
import sys
import time

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
            # Execute the SQL statement
            response = redshift_client.execute_statement(
                Database=database_name,
                WorkgroupName=workgroup_name,
                Sql=sql.strip()
            )
            
            # Wait for the query to complete
            query_id = response['Id']
            while True:
                status = redshift_client.describe_statement(Id=query_id)
                if status['Status'] in ['FINISHED', 'FAILED', 'ABORTED']:
                    break
                time.sleep(0.5)
            
            if status['Status'] == 'FAILED':
                raise Exception(f"Query failed: {status.get('Error', 'Unknown error')}\nSQL: {sql}")
            
            results.append({
                'sql': sql,
                'status': status['Status']
            })
            
        except Exception as e:
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

    sql_statements = [
        # Create external schema
        f"""
        create external schema tickit_external
        from data catalog
        database '{glue_database}'
        iam_role '{iam_role_arn}'
        create external database if not exists;
        """,
        
        # Create dbt schema
        "create schema tickit_dbt;",
        
        # Drop public schema
        "drop schema public cascade;",
        
        # Create dbt user and group
        f"create user dbt with password '{dbt_password}' nocreatedb nocreateuser syslog access restricted connection limit 10;",
        "create group dbt with user dbt;",
        
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
    ]

    try:
        results = execute_sql(sql_statements, database_name, workgroup_name)
        print("SQL initialization completed successfully")
    except Exception as e:
        print(f"Error during SQL initialization: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()