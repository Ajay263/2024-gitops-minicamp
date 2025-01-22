# modules/ec2/variables.tf
variable "project_name" {
  description = "Name of the project used for resource naming"
  type        = string
}

variable "ami_id" {
  description = "AMI ID for the EC2 instance"
  type        = string
}

variable "instance_type" {
  description = "Instance type for the EC2 instance"
  type        = string
}

variable "vpc_id" {
  description = "ID of the VPC where resources will be created"
  type        = string
}

variable "subnet_id" {
  description = "ID of the subnet where the EC2 instance will be launched"
  type        = string
}

variable "ec2_instance_profile_name" {
  description = "Name of the IAM instance profile for the EC2 instance"
  type        = string
}

variable "public_key" {
  description = "Public SSH key for EC2 instance access"
  type        = string
}
