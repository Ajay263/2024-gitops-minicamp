
# modules/s3/outputs.tf
output "source_bucket_id" {
  value = aws_s3_bucket.source_bucket.id
}

output "target_bucket_id" {
  value = aws_s3_bucket.target_bucket.id
}

output "code_bucket_id" {
  value = aws_s3_bucket.code_bucket.id
}

output "kms_key_arn" {
  description = "The ARN of the KMS key used for S3 encryption"
  value       = aws_kms_key.s3_kms_key.arn
}
