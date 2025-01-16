# modules/ec2/variables.tf

variable "project_name" {
  type        = string
  description = "Name of the project"
}

variable "ami_id" {
  type        = string
  description = "ID of the AMI to use for the EC2 instance"
}

variable "instance_type" {
  type        = string
  description = "Type of EC2 instance"
}

variable "vpc_id" {
  type        = string
  description = "ID of the VPC"
}

variable "subnet_id" {
  type        = string
  description = "ID of the subnet where the EC2 instance will be launched"
}

variable "ec2_instance_profile_name" {
  type        = string
  description = "Name of the EC2 instance profile"
}

variable "public_key" {
  type        = string
  description = "Public SSH key for EC2 instance access"
}