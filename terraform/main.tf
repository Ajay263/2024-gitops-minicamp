# main.tf
provider "aws" {
  region = var.aws_region
}

module "s3" {
  source        = "./modules/s3"
  source_bucket = var.source_bucket
  target_bucket = var.target_bucket
  code_bucket   = var.code_bucket
  environment   = var.environment
  script_path   = var.script_path
}

module "iam" {
  source      = "./modules/iam"
  environment = var.environment
}

module "glue" {
  source        = "./modules/glue"
  source_bucket = module.s3.source_bucket_id
  target_bucket = module.s3.target_bucket_id
  code_bucket   = module.s3.code_bucket_id
  glue_role_arn = module.iam.glue_role_arn
  environment   = var.environment
}
