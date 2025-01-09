# main.tf
provider "aws" {
  region = var.aws_region
}

module "s3" {
  source                    = "./modules/s3"
  source_bucket             = var.source_bucket
  target_bucket             = var.target_bucket
  code_bucket               = var.code_bucket
  environment               = var.environment
  script_path               = var.script_path
  schema_change_script_path = var.schema_change_script_path
  organizations_csv_path    = "${path.root}/modules/data/organizations.csv"
}

module "iam" {
  source      = "./modules/iam"
  environment = var.environment

}

module "sns" {
  source        = "./modules/sns"
  environment   = var.environment
  glue_role_arn = module.iam.glue_role_arn
}

module "glue" {
  source        = "./modules/glue"
  source_bucket = module.s3.source_bucket_id
  target_bucket = module.s3.target_bucket_id
  code_bucket   = module.s3.code_bucket_id
  glue_role_arn = module.iam.glue_role_arn
  environment   = var.environment
  sns_topic_arn = module.sns.topic_arn
}

# Add to your existing terraform/main.tf
module "lambda" {
  source = "./modules/lambda"

  environment   = var.environment
  source_bucket = var.source_bucket
  target_bucket = var.target_bucket

  depends_on = [module.s3]
}


module "vpc" {
  source = "./modules/vpc"

  redshift_serverless_vpc_cidr      = var.redshift_serverless_vpc_cidr
  redshift_serverless_subnet_1_cidr = var.redshift_serverless_subnet_1_cidr
  redshift_serverless_subnet_2_cidr = var.redshift_serverless_subnet_2_cidr
  redshift_serverless_subnet_3_cidr = var.redshift_serverless_subnet_3_cidr
  app_name                          = var.app_name
}

module "redshift" {
  source = "./modules/redshift"

  redshift_serverless_namespace_name      = var.redshift_serverless_namespace_name
  redshift_serverless_database_name       = var.redshift_serverless_database_name
  redshift_serverless_admin_username      = var.redshift_serverless_admin_username
  redshift_serverless_admin_password      = var.redshift_serverless_admin_password
  redshift_serverless_workgroup_name      = var.redshift_serverless_workgroup_name
  redshift_serverless_base_capacity       = var.redshift_serverless_base_capacity
  redshift_serverless_publicly_accessible = var.redshift_serverless_publicly_accessible

  redshift_role_arn = module.iam.redshift_role_arn
  security_group_id = module.vpc.security_group_id
  subnet_ids        = module.vpc.subnet_ids
}
