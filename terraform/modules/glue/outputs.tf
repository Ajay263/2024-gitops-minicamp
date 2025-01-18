
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

output "glue_etl_job_name" {
  description = "Name of the main ETL Glue job"
  value       = aws_glue_job.etl_job.name
}

output "glue_s3_to_redshift_job_name" {
  description = "Name of the S3 to Redshift Glue job"
  value       = aws_glue_job.s3_to_redshift_job.name
}
