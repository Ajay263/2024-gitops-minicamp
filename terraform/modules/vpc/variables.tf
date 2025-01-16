# modules/vpc/variables.tf
variable "redshift_serverless_vpc_cidr" {
  type = string
}

variable "redshift_serverless_subnet_1_cidr" {
  type = string
}

variable "redshift_serverless_subnet_2_cidr" {
  type = string
}

variable "redshift_serverless_subnet_3_cidr" {
  type = string
}

variable "app_name" {
  type = string
}

variable "public_key" {
  type        = string
  description = "Public SSH key for EC2 instance access"
}
