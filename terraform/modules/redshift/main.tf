# modules/redshift/main.tf

# Add this at the top to get account ID
data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

resource "aws_redshiftserverless_namespace" "serverless" {
  namespace_name      = var.redshift_serverless_namespace_name
  db_name            = var.redshift_serverless_database_name
  admin_username     = var.redshift_serverless_admin_username
  admin_user_password = var.redshift_serverless_admin_password
  iam_roles          = [var.redshift_role_arn]
  tags = {
    Name = var.redshift_serverless_namespace_name
  }
}

resource "aws_redshiftserverless_workgroup" "serverless" {
  depends_on = [aws_redshiftserverless_namespace.serverless]
  namespace_name = aws_redshiftserverless_namespace.serverless.id
  workgroup_name = var.redshift_serverless_workgroup_name
  base_capacity  = var.redshift_serverless_base_capacity
  security_group_ids = [var.security_group_id]
  subnet_ids = var.subnet_ids
  publicly_accessible = var.redshift_serverless_publicly_accessible
  tags = {
    Name = var.redshift_serverless_workgroup_name
  }
}

# Add resource to execute SQL initialization
# modules/redshift/main.tf

# modules/redshift/main.tf

resource "terraform_data" "sql_init" {
  depends_on = [aws_redshiftserverless_workgroup.serverless]

  triggers_replace = {
    namespace_id = aws_redshiftserverless_namespace.serverless.id
    workgroup_id = aws_redshiftserverless_workgroup.serverless.id
    script_hash  = filemd5("${path.module}/sql-init.py")
  }

  provisioner "local-exec" {
    command = <<-EOT
      cd ${path.module} && python3 sql-init.py \
      "${var.redshift_serverless_database_name}" \
      "${var.redshift_serverless_workgroup_name}" \
      "${var.redshift_role_arn}" \
      "${var.dbt_password}"
    EOT

    environment = {
      AWS_DEFAULT_REGION = data.aws_region.current.name
    }
  }
}