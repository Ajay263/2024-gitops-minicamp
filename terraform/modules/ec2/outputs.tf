# modules/ec2/outputs.tf
output "instance_id" {
  value = aws_instance.rag_cs_instance.id
}

output "instance_public_ip" {
  value = aws_instance.rag_cs_instance.public_ip
}