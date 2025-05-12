import os
import sys
import time

import boto3


def wait_for_workgroup(redshift_client, workgroup_name, max_attempts=30):
    """
    Wait for Redshift Serverless workgroup to be available
    """
    print(f"Waiting for workgroup {workgroup_name} to be available...")
    for attempt in range(max_attempts):
        try:
            response = redshift_client.get_workgroup(workgroupName=workgroup_name)
            status = response["workgroup"]["status"]
            print(f"Workgroup status: {status}")

            if status == "AVAILABLE":
                print("Workgroup is available!")
                return True

            if status in ["FAILED", "ERROR"]:
                print(f"Workgroup failed to become available: {status}")
                return False

        except Exception as e:
            print(f"Error checking workgroup status: {str(e)}")

        time.sleep(10)  # Wait 10 seconds between checks

    print(f"Timed out waiting for workgroup after {max_attempts} attempts")
    return False


def check_object_exists(redshift_client, database_name, workgroup_name, query):
    """
    Check the existence of  an object  in Redshift
    """
    try:
        response = redshift_client.execute_statement(
            Database=database_name, WorkgroupName=workgroup_name, Sql=query
        )

        query_id = response["Id"]
        while True:
            status = redshift_client.describe_statement(Id=query_id)
            if status["Status"] in ["FINISHED", "FAILED", "ABORTED"]:
                break
            time.sleep(0.5)

        if status["Status"] == "FINISHED":
            result = redshift_client.get_statement_result(Id=query_id)
            return len(result.get("Records", [])) > 0

        return False
    except Exception as e:
        print(f"Error checking object existence: {str(e)}")
        return False


def execute_sql(sql_statements, database_name, workgroup_name, max_retries=3):
    """
    Execute SQL statements in Redshift Serverless with retries
    """
    redshift_client = boto3.client("redshift-data")

    for sql in sql_statements:
        if not sql.strip():
            continue

        retry_count = 0
        while retry_count < max_retries:
            try:
                print(f"Executing SQL: {sql.strip()}")
                response = redshift_client.execute_statement(
                    Database=database_name,
                    WorkgroupName=workgroup_name,
                    Sql=sql.strip(),
                )

                query_id = response["Id"]
                while True:
                    status = redshift_client.describe_statement(Id=query_id)
                    if status["Status"] in ["FINISHED", "FAILED", "ABORTED"]:
                        break
                    time.sleep(0.5)

                if status["Status"] == "FAILED":
                    error_message = status.get("Error", "Unknown error")
                    if "already exists" in error_message.lower():
                        break  # Skip to next SQL statement
                    print(f"Query failed: {error_message}\nSQL: {sql}")
                    retry_count += 1
                    if retry_count == max_retries:
                        raise Exception(
                            f"Query failed after {max_retries} retries: {error_message}"
                        )
                    time.sleep(5)  
                    continue

                break  

            except Exception as e:
                if "already exists" in str(e).lower():
                    break
                print(f"Error executing SQL: {str(e)}")
                retry_count += 1
                if retry_count == max_retries:
                    raise
                time.sleep(5)  


def main():
    if len(sys.argv) != 5:
        print(
            "Usage: script.py <database_name> <workgroup_name> <iam_role_arn> <dbt_password>"
        )
        sys.exit(1)

    database_name = sys.argv[1]
    workgroup_name = sys.argv[2]
    iam_role_arn = sys.argv[3]
    dbt_password = sys.argv[4]

    print(f"Working directory: {os.getcwd()}")
    print(f"Database: {database_name}")
    print(f"Workgroup: {workgroup_name}")
    print(f"IAM Role: {iam_role_arn}")

    redshift_client = boto3.client("redshift-serverless")
    if not wait_for_workgroup(redshift_client, workgroup_name):
        print("Failed to wait for workgroup to become available")
        sys.exit(1)

    redshift_data_client = boto3.client("redshift-data")

    external_schema_exists = check_object_exists(
        redshift_data_client,
        database_name,
        workgroup_name,
        "SELECT 1 FROM pg_namespace WHERE nspname = 'nexabrands_external'",
    )

    dbt_schema_exists = check_object_exists(
        redshift_data_client,
        database_name,
        workgroup_name,
        "SELECT 1 FROM pg_namespace WHERE nspname = 'nexabrands_dbt'",
    )

    public_schema_exists = check_object_exists(
        redshift_data_client,
        database_name,
        workgroup_name,
        "SELECT 1 FROM pg_namespace WHERE nspname = 'public'",
    )

    user_exists = check_object_exists(
        redshift_data_client,
        database_name,
        workgroup_name,
        "SELECT 1 FROM pg_user WHERE usename = 'dbt'",
    )

    sql_statements = []

    if not external_schema_exists:
        sql_statements.append(
            f"""
            CREATE EXTERNAL SCHEMA nexabrands_external
            FROM DATA CATALOG
            DATABASE 'nexabrands_dbt'
            IAM_ROLE '{iam_role_arn}'
            CREATE EXTERNAL DATABASE IF NOT EXISTS;
        """
        )

    if not dbt_schema_exists:
        sql_statements.append("CREATE SCHEMA nexabrands_dbt;")

    if public_schema_exists:
        sql_statements.append("DROP SCHEMA public CASCADE;")

    if not user_exists:
        sql_statements.extend(
            [
                f"""
            CREATE USER dbt WITH PASSWORD '{dbt_password}'
            NOCREATEDB NOCREATEUSER SYSLOG ACCESS RESTRICTED
            CONNECTION LIMIT 10;
            """,
                "CREATE GROUP dbt;",
                "ALTER GROUP dbt ADD USER dbt;",
            ]
        )

    sql_statements.extend(
        [
         
            "GRANT USAGE ON SCHEMA nexabrands_external TO GROUP dbt;",
            "GRANT CREATE ON SCHEMA nexabrands_external TO GROUP dbt;",
            "GRANT ALL ON ALL TABLES IN SCHEMA nexabrands_external TO GROUP dbt;",
            "GRANT USAGE ON SCHEMA nexabrands_dbt TO GROUP dbt;",
            "GRANT CREATE ON SCHEMA nexabrands_dbt TO GROUP dbt;",
            "GRANT ALL ON ALL TABLES IN SCHEMA nexabrands_dbt TO GROUP dbt;",
            "ALTER SCHEMA nexabrands_dbt OWNER TO dbt;",
            "ALTER SCHEMA nexabrands_external OWNER TO dbt;",
        ]
    )

    try:
        execute_sql(sql_statements, database_name, workgroup_name)
        print("Setup completed successfully!")
    except Exception as e:
        print(f"Setup failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
