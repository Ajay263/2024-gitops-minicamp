output "vpc_id" {
  description = "The ID of the VPC"
  value       = aws_vpc.redshift-serverless-vpc.id
}

output "security_group_id" {
  description = "The ID of the security group"
  value       = aws_security_group.redshift-serverless-security-group.id
}

output "public_subnet_az1_id" {
  description = "The ID of the public subnet in AZ1"
  value       = aws_subnet.public_subnet_az1.id
}

output "public_subnet_id" {
  description = "The ID of the public subnet (alias for public_subnet_az1_id)"
  value       = aws_subnet.public_subnet_az1.id
}

output "subnet_ids" {
  description = "List of all subnet IDs"
  value = [
    aws_subnet.redshift-serverless-subnet-az1.id,
    aws_subnet.redshift-serverless-subnet-az2.id,
    aws_subnet.redshift-serverless-subnet-az3.id,
    aws_subnet.public_subnet_az1.id
  ]
}

# modules/redshift/main.tf
output "redshift_endpoint" {
  value = aws_redshiftserverless_workgroup.serverless.endpoint[0].address
}