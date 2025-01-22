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