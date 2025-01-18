
# modules/vpc/outputs.tf
output "vpc_id" {
  description = "ID of the VPC"
  value       = aws_vpc.redshift-serverless-vpc.id
}

output "public_subnet_id" {
  description = "ID of the public subnet"
  value       = aws_subnet.public_subnet_az1.id
}

output "subnet_ids" {
  description = "List of subnet IDs"
  value = [
    aws_subnet.redshift-serverless-subnet-az1.id,
    aws_subnet.redshift-serverless-subnet-az2.id,
    aws_subnet.redshift-serverless-subnet-az3.id
  ]
}

output "security_group_id" {
  description = "ID of the security group"
  value       = aws_security_group.redshift-serverless-security-group.id
}