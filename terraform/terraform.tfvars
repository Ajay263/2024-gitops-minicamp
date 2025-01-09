# AWS and Environment Configuration
aws_region                = "us-east-1"
environment               = "prod"
source_bucket             = "source-data"
target_bucket             = "target-data"
code_bucket               = "code-bucket"
script_path               = "./script.py"
schema_change_script_path = "./schema_change.py"

# Application Configuration
app_name = "nsw-properties"

# Network Configuration
redshift_serverless_vpc_cidr      = "10.0.0.0/16"
redshift_serverless_subnet_1_cidr = "10.0.1.0/24"
redshift_serverless_subnet_2_cidr = "10.0.2.0/24"
redshift_serverless_subnet_3_cidr = "10.0.3.0/24"

# Redshift Configuration
redshift_serverless_namespace_name      = "nsw-properties-redshift"
redshift_serverless_database_name       = "nsw_properties_db"
redshift_serverless_admin_username      = "admin"
redshift_serverless_admin_password      = "YourSecurePassword123!"
redshift_serverless_workgroup_name      = "nsw-properties-workgroup"
redshift_serverless_base_capacity       = 32
redshift_serverless_publicly_accessible = false