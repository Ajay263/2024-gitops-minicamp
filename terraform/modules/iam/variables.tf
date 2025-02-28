variable "source_bucket" {
  type = string
}

variable "target_bucket" {
  type = string
}

variable "code_bucket" {
  type = string
}

variable "sns_topic_arn" {
  type = string
}

# modules/iam/variables.tf
variable "environment" {
  description = "Environment (dev/staging/prod)"
  type        = string
}

variable "kms_key_arn" {
  description = "The ARN of the KMS key used for S3 encryption"
  type        = string
}




variable "aws_region" {
  description = "AWS region"
  type        = string
  default     =  "us-east-1"
}

variable "redshift_cluster_arn" {
  description = "ARN of the Redshift cluster"
  type        = string
}