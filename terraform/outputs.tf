
# outputs.tf
output "source_bucket_name" {
  description = "Name of the source S3 bucket"
  value       = module.s3.source_bucket_id
}

output "target_bucket_name" {
  description = "Name of the target S3 bucket"
  value       = module.s3.target_bucket_id
}

output "code_bucket_name" {
  description = "Name of the code S3 bucket"
  value       = module.s3.code_bucket_id
}

output "glue_database_name" {
  description = "Name of the Glue catalog database"
  value       = module.glue.glue_database_name
}

output "glue_crawler_name" {
  description = "Name of the Glue crawler"
  value       = module.glue.glue_crawler_name
}

output "glue_etl_job_name" {
  description = "Name of the Glue ETL job"
  value       = module.glue.glue_etl_job_name
}

output "redshift_endpoint" {
  description = "Endpoint for Redshift Serverless workgroup"
  value       = module.redshift.redshift_endpoint
}

output "ec2_instance_public_ip" {
  description = "Public IP address of the EC2 instance"
  value       = module.ec2.instance_public_ip
}

output "vpc_id" {
  description = "ID of the created VPC"
  value       = module.vpc.vpc_id
}

output "subnet_ids" {
  description = "IDs of the created subnets"
  value       = module.vpc.subnet_ids
}
