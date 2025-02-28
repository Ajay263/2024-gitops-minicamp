variable "redshift_serverless_vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
}

variable "redshift_serverless_subnet_1_cidr" {
  description = "CIDR block for the first Redshift subnet"
  type        = string
}

variable "redshift_serverless_subnet_2_cidr" {
  description = "CIDR block for the second Redshift subnet"
  type        = string
}

variable "redshift_serverless_subnet_3_cidr" {
  description = "CIDR block for the third Redshift subnet"
  type        = string
}

variable "app_name" {
  description = "Name of the application"
  type        = string
}

variable "public_key" {
  description = "Public key for SSH access"
  type        = string
}

variable "redshift_cluster_arn" {
  description = "ARN of the Redshift Serverless workgroup"
  type        = string
}

