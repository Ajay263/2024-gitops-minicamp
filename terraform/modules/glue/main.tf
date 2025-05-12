# Current AWS account ID
data "aws_caller_identity" "current" {}

# Glue Catalog Database for Reports
resource "aws_glue_catalog_database" "database" {
  name        = "topdevs-${var.environment}-report"
  description = "Database for ${var.environment} environment organization reports"
}

# External Glue Catalog Database
resource "aws_glue_catalog_database" "external" {
  name        = "nexabrands_dbt"
  description = "External database for Redshift Spectrum tables"
}

# Glue Crawler
resource "aws_glue_crawler" "crawler" {
  name          = "topdevs-${var.environment}-org-report-crawler"
  database_name = aws_glue_catalog_database.database.name
  role          = var.glue_role_arn

  s3_target {
    path = "s3://${var.source_bucket}/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })
}

# Local variable for Glue Jobs
locals {
  jobs = {
    products          = "products"
    orders            = "orders"
    order_lines       = "order_lines"
    order_fulfillment = "order_fulfillment"
    customers         = "customers"
    customer_targets  = "customer_targets"
  }
  
}

# Glue ETL Jobs
resource "aws_glue_job" "etl_jobs" {
  for_each = local.jobs

  name              = "topdevs-${var.environment}-${each.value}-job"
  role_arn          = var.glue_role_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 2880
  max_retries       = 1

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://nexabrand-${var.environment}-${var.code_bucket}/scripts/${each.value}.py"
  }

  default_arguments = {
    "--enable-auto-scaling"              = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--source-path"                      = "s3://nexabrand-${var.environment}-${var.source_bucket}/"
    "--destination-path"                 = "s3://nexabrand-${var.environment}-${var.target_bucket}/"
    "--job-name"                         = "topdevs-${var.environment}-${each.value}-job"
    "--enable-metrics"                   = "true"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = {
    Environment = var.environment
    Service     = "glue"
  }
}
