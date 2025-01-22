########################
## AWS - Variables ##
########################
variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment (dev/staging/prod)"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod"
  }
}

########################
## S3 - Variables ##
########################
variable "source_bucket" {
  description = "Name suffix of the source data bucket (will be prefixed with nexabrands-{environment}-)"
  type        = string
}

variable "target_bucket" {
  description = "Name suffix of the target data bucket (will be prefixed with nexabrands-{environment}-)"
  type        = string
}

variable "code_bucket" {
  description = "Name suffix of the code bucket (will be prefixed with nexabrands-{environment}-)"
  type        = string
}

# S3 Configuration Variables
variable "kms_deletion_window" {
  description = "Duration in days before KMS key is deleted"
  type        = number
  default     = 7
}

variable "lifecycle_ia_transition_days" {
  description = "Number of days before transitioning non-current versions to STANDARD_IA storage"
  type        = number
  default     = 30
}

variable "lifecycle_glacier_transition_days" {
  description = "Number of days before transitioning non-current versions to GLACIER storage"
  type        = number
  default     = 60
}

variable "lifecycle_expiration_days" {
  description = "Number of days before deleting non-current versions"
  type        = number
  default     = 90
}

variable "object_lock_retention_days" {
  description = "Number of days for S3 Object Lock retention period in COMPLIANCE mode"
  type        = number
  default     = 1
}

#############################
## Application - Variables ##
#############################
variable "app_name" {
  type        = string
  description = "Application name"
  default     = "nexabrands"
}

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "nexabrands"
}

#########################
## Network - Variables ##
#########################
variable "redshift_serverless_vpc_cidr" {
  type        = string
  description = "VPC IPv4 CIDR"
  default     = "10.0.0.0/16"
}

variable "redshift_serverless_subnet_1_cidr" {
  type        = string
  description = "IPv4 CIDR for Redshift subnet 1"
  default     = "10.0.1.0/24"
}

variable "redshift_serverless_subnet_2_cidr" {
  type        = string
  description = "IPv4 CIDR for Redshift subnet 2"
  default     = "10.0.2.0/24"
}

variable "redshift_serverless_subnet_3_cidr" {
  type        = string
  description = "IPv4 CIDR for Redshift subnet 3"
  default     = "10.0.3.0/24"
}

#####################################
## Redshift Serverless - Variables ##
#####################################
variable "redshift_serverless_namespace_name" {
  type        = string
  description = "Redshift Serverless Namespace Name"
  default     = "nexabrands-redshift-namespace"
}

variable "redshift_serverless_database_name" {
  type        = string
  description = "Redshift Serverless Database Name"
  default     = "nexabrands_datawarehouse"
}

variable "redshift_serverless_admin_username" {
  type        = string
  description = "Redshift Serverless Admin Username"
  sensitive   = true
}

variable "redshift_serverless_admin_password" {
  type        = string
  description = "Redshift Serverless Admin Password"
  sensitive   = true
}

variable "redshift_serverless_workgroup_name" {
  type        = string
  description = "Redshift Serverless Workgroup Name"
  default     = "nexabrands-redshift-workgroup"
}

variable "redshift_serverless_base_capacity" {
  type        = number
  description = "Redshift Serverless Base Capacity (32-512 RPUs in units of 8)"
  default     = 32
}

variable "redshift_serverless_publicly_accessible" {
  type        = bool
  description = "Set the Redshift Serverless to be Publicly Accessible"
  default     = false
}

#######################
## EC2 - Variables ##
#######################
variable "ami_id" {
  type        = string
  description = "ID of the AMI to use for the EC2 instance"
  default     = "ami-0e2c8caa4b6378d8c"
}

variable "instance_type" {
  type        = string
  description = "Type of EC2 instance"
  default     = "t3.large"
}

variable "public_key" {
  type        = string
  description = "Public SSH key for EC2 instance access"
  sensitive   = true
}

variable "dbt_password" {
  type        = string
  sensitive   = true
  description = "Password for the dbt user"
}

variable "glue_database_name" {
  type        = string
  description = "Name of the Glue database to connect to"
  default     = "tickit_dbt"
}


variable "source_files" {
  description = "Map of source files to upload to S3"
  type        = map(string)
  default = {
    "customers.csv"         = "./modules/data/customers.csv"
    "customer_targets.csv"  = "./modules/data/customer_targets.csv"
    "dates.csv"             = "./modules/data/dates.csv"
    "orders.csv"            = "./modules/data/orders.csv"
    "order_fulfillment.csv" = "./modules/data/order_fulfillment.csv"
    "order_lines.csv"       = "./modules/data/order_lines.csv"
    "products.csv"          = "./modules/data/products.csv"
  }
}

variable "code_files" {
  description = "Map of code file names to their local paths"
  type        = map(string)
  default = {
    "script.py"         = "./modules/scripts/script.py"
    "schema_change.py"  = "./modules/scripts/schema_change.py"
    "s3_to_redshift.py" = "./modules/scripts/s3_to_redshift.py"
  }
}

