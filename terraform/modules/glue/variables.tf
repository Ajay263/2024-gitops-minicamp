
# modules/glue/variables.tf
variable "environment" {
  description = "Environment name (e.g., dev, staging, prod)"
  type        = string
}

variable "source_bucket" {
  description = "Name of the S3 bucket containing source data"
  type        = string
}

variable "target_bucket" {
  description = "Name of the S3 bucket for processed data"
  type        = string
}

variable "code_bucket" {
  description = "Name of the S3 bucket containing Glue scripts"
  type        = string
}

variable "glue_role_arn" {
  description = "ARN of the IAM role for Glue services"
  type        = string
}

variable "redshift_database" {
  description = "Name of the Redshift database"
  type        = string
}

variable "redshift_schema" {
  description = "Name of the Redshift schema"
  type        = string
}

variable "redshift_workgroup_name" {
  description = "Name of the Redshift Serverless workgroup"
  type        = string
}
