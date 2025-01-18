
# modules/ec2/outputs.tf
output "instance_id" {
  description = "ID of the created EC2 instance"
  value       = aws_instance.rag_cs_instance.id
}

output "instance_public_ip" {
  description = "Public IP address of the EC2 instance"
  value       = aws_instance.rag_cs_instance.public_ip
}

output "security_group_id" {
  description = "ID of the security group created for the EC2 instance"
  value       = aws_security_group.rag_cs_sg.id
}
