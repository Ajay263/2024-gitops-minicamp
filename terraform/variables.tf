# variables.tf

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
}

########################
## S3 - Variables ##
########################
variable "source_bucket" {
  description = "Name of the source data bucket"
  type        = string

}

variable "target_bucket" {
  description = "Name of the target data bucket"
  type        = string

}

variable "code_bucket" {
  description = "Name of the code bucket"
  type        = string

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













