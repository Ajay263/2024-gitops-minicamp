
# terraform.tfvars
aws_region    = "us-east-1"
environment   = "prod"
source_bucket = "source"
target_bucket = "target"
code_bucket   = "code"
app_name      = "nexabrands"
project_name  = "nexabrands"

redshift_serverless_vpc_cidr      = "10.0.0.0/16"
redshift_serverless_subnet_1_cidr = "10.0.1.0/24"
redshift_serverless_subnet_2_cidr = "10.0.2.0/24"
redshift_serverless_subnet_3_cidr = "10.0.3.0/24"

redshift_serverless_namespace_name      = "nexabrands-redshift-namespace"
redshift_serverless_database_name       = "nexabrands_datawarehouse"
redshift_serverless_admin_username      = "admin"
redshift_serverless_admin_password      = "Password123!" # Move to AWS Secrets Manager
redshift_serverless_workgroup_name      = "nexabrands-redshift-workgroup"
redshift_serverless_base_capacity       = 32
redshift_serverless_publicly_accessible = false

ami_id        = "ami-0e2c8caa4b6378d8c"
instance_type = "t3.xlarge"

