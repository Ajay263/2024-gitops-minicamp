
# modules/redshift/variables.tf
variable "redshift_serverless_namespace_name" {
  description = "Name of the Redshift Serverless namespace"
  type        = string
}

variable "redshift_serverless_database_name" {
  description = "Name of the Redshift Serverless database"
  type        = string
}

variable "redshift_serverless_admin_username" {
  description = "Admin username for Redshift Serverless"
  type        = string
  sensitive   = true
}

variable "redshift_serverless_admin_password" {
  description = "Admin password for Redshift Serverless"
  type        = string
  sensitive   = true
}

variable "redshift_role_arn" {
  description = "ARN of the IAM role for Redshift"
  type        = string
}

variable "redshift_serverless_workgroup_name" {
  description = "Name of the Redshift Serverless workgroup"
  type        = string
}

variable "redshift_serverless_base_capacity" {
  description = "Base capacity for Redshift Serverless in RPUs"
  type        = number
}

variable "security_group_id" {
  description = "ID of the security group for Redshift"
  type        = string
}

variable "subnet_ids" {
  description = "List of subnet IDs for Redshift deployment"
  type        = list(string)
}

variable "redshift_serverless_publicly_accessible" {
  description = "Whether the Redshift cluster should be publicly accessible"
  type        = bool
  default     = false
}
