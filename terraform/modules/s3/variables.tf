variable "environment" {
  description = "Environment (dev/staging/prod)"
  type        = string
}

variable "source_bucket" {
  description = "Name suffix of the source data bucket"
  type        = string
}

variable "target_bucket" {
  description = "Name suffix of the target data bucket"
  type        = string
}

variable "code_bucket" {
  description = "Name suffix of the code bucket"
  type        = string
}

variable "kms_deletion_window" {
  description = "Duration in days before KMS key is deleted"
  type        = number
  default     = 7
}

variable "lifecycle_ia_transition_days" {
  description = "Number of days before transitioning to IA"
  type        = number
  default     = 30
}

variable "lifecycle_glacier_transition_days" {
  description = "Number of days before transitioning to Glacier"
  type        = number
  default     = 60
}

variable "lifecycle_expiration_days" {
  description = "Number of days before expiration"
  type        = number
  default     = 90
}

variable "object_lock_retention_days" {
  description = "Number of days for Object Lock retention"
  type        = number
  default     = 1
}

variable "source_files" {
  description = "Map of source files to upload"
  type        = map(string)
}

variable "code_files" {
  description = "Map of code files to upload"
  type        = map(string)
}
