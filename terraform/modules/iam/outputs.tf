output "redshift_role_arn" {
  value = aws_iam_role.redshift-serverless-role.arn
}
output "ec2_instance_profile_name" {
  value = aws_iam_instance_profile.ec2_profile.name
}

output "glue_role_arn" {
  description = "The ARN of the Glue service role"
  value       = aws_iam_role.glue_service_role.arn
}

output "ec2_role_arn" {
  description = "The ARN of the EC2 role"
  value       = aws_iam_role.ec2_role.arn
}
