# Source Bucket
resource "aws_s3_bucket" "source_bucket" {
  bucket              = "nexabrands-${var.environment}-${var.source_bucket}"
  force_destroy       = true
}

# Enable versioning for source bucket
resource "aws_s3_bucket_versioning" "source_bucket_versioning" {
  bucket = aws_s3_bucket.source_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Upload objects to source bucket
resource "aws_s3_object" "source_files" {
  for_each = var.source_files

  bucket                 = aws_s3_bucket.source_bucket.id
  key                    = "data/${each.key}"
  source                 = each.value
  server_side_encryption = "aws:kms"
  kms_key_id            = aws_kms_key.s3_kms_key.arn

  depends_on = [
    aws_s3_bucket_versioning.source_bucket_versioning
  ]
}

# Target Bucket
resource "aws_s3_bucket" "target_bucket" {
  bucket              = "nexabrands-${var.environment}-${var.target_bucket}"
  force_destroy       = true
}

# Enable versioning for target bucket
resource "aws_s3_bucket_versioning" "target_bucket_versioning" {
  bucket = aws_s3_bucket.target_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Code Bucket
resource "aws_s3_bucket" "code_bucket" {
  bucket        = "nexabrands-${var.environment}-${var.code_bucket}"
  force_destroy = true
}

# Enable versioning for code bucket
resource "aws_s3_bucket_versioning" "code_bucket_versioning" {
  bucket = aws_s3_bucket.code_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Upload code files
resource "aws_s3_object" "code_files" {
  for_each = var.code_files

  bucket                 = aws_s3_bucket.code_bucket.id
  key                    = "scripts/${each.key}"
  source                 = each.value
  server_side_encryption = "aws:kms"
  kms_key_id            = aws_kms_key.s3_kms_key.arn

  depends_on = [aws_s3_bucket_versioning.code_bucket_versioning]
}

# KMS Key for Server-Side Encryption
resource "aws_kms_key" "s3_kms_key" {
  description             = "KMS key for S3 bucket encryption"
  deletion_window_in_days = var.kms_deletion_window
  enable_key_rotation     = true

  tags = {
    Environment = var.environment
    Purpose     = "s3-encryption"
  }
}

resource "aws_kms_alias" "s3_kms_alias" {
  name          = "alias/s3-encryption-key-${var.environment}"
  target_key_id = aws_kms_key.s3_kms_key.key_id
}

# Enable Server-Side Encryption for all buckets
resource "aws_s3_bucket_server_side_encryption_configuration" "source_bucket_encryption" {
  bucket = aws_s3_bucket.source_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.s3_kms_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "target_bucket_encryption" {
  bucket = aws_s3_bucket.target_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.s3_kms_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "code_bucket_encryption" {
  bucket = aws_s3_bucket.code_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.s3_kms_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

# Lifecycle Rules for source and target buckets only,
resource "aws_s3_bucket_lifecycle_configuration" "source_bucket_lifecycle" {
  bucket = aws_s3_bucket.source_bucket.id

  rule {
    id     = "transition_noncurrent_versions"
    status = "Enabled"

    noncurrent_version_transition {
      noncurrent_days = var.lifecycle_ia_transition_days
      storage_class   = "STANDARD_IA"
    }

    noncurrent_version_transition {
      noncurrent_days = var.lifecycle_glacier_transition_days
      storage_class   = "GLACIER"
    }

    noncurrent_version_expiration {
      noncurrent_days = var.lifecycle_expiration_days
    }
  }

  depends_on = [aws_s3_bucket_versioning.source_bucket_versioning]
}

resource "aws_s3_bucket_lifecycle_configuration" "target_bucket_lifecycle" {
  bucket = aws_s3_bucket.target_bucket.id

  rule {
    id     = "transition_noncurrent_versions"
    status = "Enabled"

    noncurrent_version_transition {
      noncurrent_days = var.lifecycle_ia_transition_days
      storage_class   = "STANDARD_IA"
    }

    noncurrent_version_transition {
      noncurrent_days = var.lifecycle_glacier_transition_days
      storage_class   = "GLACIER"
    }

    noncurrent_version_expiration {
      noncurrent_days = var.lifecycle_expiration_days
    }
  }

  depends_on = [aws_s3_bucket_versioning.target_bucket_versioning]
}
