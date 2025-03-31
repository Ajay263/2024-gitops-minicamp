# modules/glue/outputs.tf

output "glue_database_name" {
  description = "Name of the created Glue catalog database"
  value       = aws_glue_catalog_database.database.name
}

output "glue_external_database_name" {
  description = "Name of the external Glue catalog database"
  value       = aws_glue_catalog_database.external.name
}

output "glue_crawler_name" {
  description = "Name of the Glue crawler"
  value       = aws_glue_crawler.crawler.name
}

output "glue_job_names" {
  description = "Names of the created Glue ETL jobs"
  value       = { for k, v in aws_glue_job.etl_jobs : k => v.name }
}

output "glue_job_arns" {
  description = "ARNs of the created Glue ETL jobs"
  value       = { for k, v in aws_glue_job.etl_jobs : k => v.arn }
}

output "data_quality_job_names" {
  description = "Names of the data quality testing jobs"
  value       = { for k, v in aws_glue_job.data_quality_jobs : k => v.name }
}

output "data_quality_job_arns" {
  description = "ARNs of the data quality testing jobs"
  value       = { for k, v in aws_glue_job.data_quality_jobs : k => v.arn }
}