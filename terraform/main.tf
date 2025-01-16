# main.tf
provider "aws" {
  region = var.aws_region
}

module "s3" {
  source = "./modules/s3"

  environment   = var.environment
  source_bucket = var.source_bucket
  target_bucket = var.target_bucket
  code_bucket   = var.code_bucket

  source_files = {
    "customers.csv" = "${path.root}/modules/data/customers.csv"
    "products.csv"  = "${path.root}/modules/data/products.csv"
    "date.csv"      = "${path.root}/modules/data/date.csv"
  }

  code_files = {
    "script.py"        = "${path.root}/modules/scripts/script.py"
    "schema_change.py" = "${path.root}/modules/scripts/schema_change.py"
  }
}

module "sns" {
  source      = "./modules/sns"
  environment = var.environment
}

module "iam" {
  source        = "./modules/iam"
  environment   = var.environment
  source_bucket = var.source_bucket
  target_bucket = var.target_bucket
  code_bucket   = var.code_bucket
  sns_topic_arn = module.sns.topic_arn
}

module "vpc" {
  source = "./modules/vpc"

  redshift_serverless_vpc_cidr      = var.redshift_serverless_vpc_cidr
  redshift_serverless_subnet_1_cidr = var.redshift_serverless_subnet_1_cidr
  redshift_serverless_subnet_2_cidr = var.redshift_serverless_subnet_2_cidr
  redshift_serverless_subnet_3_cidr = var.redshift_serverless_subnet_3_cidr
  app_name                          = var.app_name
  public_key                        = var.public_key
}

module "glue" {
  source = "./modules/glue"

  environment   = var.environment
  source_bucket = module.s3.source_bucket_id
  target_bucket = module.s3.target_bucket_id
  code_bucket   = module.s3.code_bucket_id
  glue_role_arn = module.iam.glue_role_arn
  sns_topic_arn = module.sns.topic_arn

  redshift_database       = var.redshift_serverless_database_name
  redshift_workgroup_name = var.redshift_serverless_workgroup_name

  depends_on = [module.s3, module.iam, module.sns]
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

  depends_on = [module.vpc, module.iam]
}

module "ec2" {
  source = "./modules/ec2"

  project_name              = var.project_name
  ami_id                    = var.ami_id
  instance_type             = var.instance_type
  vpc_id                    = module.vpc.vpc_id
  subnet_id                 = module.vpc.public_subnet_id
  ec2_instance_profile_name = module.iam.ec2_instance_profile_name
  public_key                = var.public_key

  depends_on = [module.vpc, module.iam]
}
