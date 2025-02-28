# Add this at the top of your file
data "aws_caller_identity" "current" {}


resource "aws_iam_role" "glue_service_role" {
  name = "topdevs-${var.environment}-glue-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
          AWS = [aws_iam_role.ec2_role.arn]  # Add this line
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name = "topdevs-${var.environment}-glue-service-role"
  }
}

# Glue Service Role Policy
resource "aws_iam_role_policy" "glue_service_policy" {
  name = "topdevs-${var.environment}-glue-service-policy"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Existing Glue actions
      {
        Effect = "Allow"
        Action = ["glue:*"]
        Resource = ["*"]
      },
      # Updated S3 actions with explicit GetObject for code bucket
      {
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation",
          "s3:ListBucket",
          "s3:GetBucketAcl",
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "arn:aws:s3:::nexabrands-${var.environment}-${var.source_bucket}",
          "arn:aws:s3:::nexabrands-${var.environment}-${var.source_bucket}/*",
          "arn:aws:s3:::nexabrands-${var.environment}-${var.target_bucket}",
          "arn:aws:s3:::nexabrands-${var.environment}-${var.target_bucket}/*",
          "arn:aws:s3:::nexabrands-${var.environment}-${var.code_bucket}",
          "arn:aws:s3:::nexabrands-${var.environment}-${var.code_bucket}/*"
        ]
      },
      # Add this new statement for Glue and logs bucket access
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:GetBucketAcl"
        ]
        Resource = [
          "arn:aws:s3:::aws-glue-*",
          "arn:aws:s3:::aws-glue-*/*",
          "arn:aws:s3:::*aws-logs*",
          "arn:aws:s3:::*aws-logs*/*"
        ]
      },
      # KMS actions for encryption/decryption
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:Encrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = [var.kms_key_arn]
      }
    ]
  })
}


# Redshift S3 Access Policy
resource "aws_iam_role_policy" "redshift-s3-access-policy" {
  name = "topdevs-${var.environment}-redshift-serverless-role-s3-policy"
  role = aws_iam_role.redshift-serverless-role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation",
          "s3:ListBucket",
          "s3:GetObject",
          "s3:PutObject"
        ]
        Resource = [
          "arn:aws:s3:::nexabrands-${var.environment}-${var.source_bucket}",
          "arn:aws:s3:::nexabrands-${var.environment}-${var.source_bucket}/*",
          "arn:aws:s3:::nexabrands-${var.environment}-${var.target_bucket}",
          "arn:aws:s3:::nexabrands-${var.environment}-${var.target_bucket}/*",
          "arn:aws:s3:::nexabrands-${var.environment}-${var.code_bucket}",
          "arn:aws:s3:::nexabrands-${var.environment}-${var.code_bucket}/*"
        ]
      }
    ]
  })
}

# Redshift Glue Access Policy
resource "aws_iam_role_policy" "redshift-glue-access-policy" {
  name = "topdevs-${var.environment}-redshift-serverless-role-glue-policy"
  role = aws_iam_role.redshift-serverless-role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Existing Glue permissions
      {
        Effect = "Allow"
        Action = [
          "glue:GetCrawler",
          "glue:StartCrawler",
          "glue:GetCrawlers",
          "glue:BatchGetCrawlers",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:BatchGetPartition",
          "glue:GetUserDefinedFunction",
          "glue:GetUserDefinedFunctions",
          "glue:GetCatalogImportStatus",
          "glue:GetConnection",
          "glue:GetConnections",
          "glue:CreateDatabase",
          "glue:DeleteDatabase",
          "glue:UpdateDatabase",
          "glue:CreateTable",
          "glue:DeleteTable",
          "glue:BatchDeleteTable",
          "glue:UpdateTable",
          "glue:BatchCreatePartition",
          "glue:CreatePartition",
          "glue:DeletePartition",
          "glue:BatchDeletePartition",
          "glue:UpdatePartition"
        ]
        Resource = [
          "arn:aws:glue:*:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:*:${data.aws_caller_identity.current.account_id}:crawler/*",
          "arn:aws:glue:*:${data.aws_caller_identity.current.account_id}:database/*",
          "arn:aws:glue:*:${data.aws_caller_identity.current.account_id}:table/*",
          "arn:aws:glue:*:${data.aws_caller_identity.current.account_id}:connection/*"
        ]
      },
      # New Redshift Query Editor Full Access Permissions
      {
        Effect = "Allow"
        Action = [
          # Redshift Query Editor V2 Specific Permissions
          "redshift:DescribeQueryEditorV2",
          "redshift:GetQueryEditorV2Results",
          "redshift:CreateQueryEditorV2Favorites",
          "redshift:DeleteQueryEditorV2Favorites",
          "redshift:ListQueryEditorV2Favorites",
          "redshift:BatchExecuteQueryEditorQuery",
          "redshift:UpdateQueryEditorV2Favorites",
          
          # Additional Query and Cluster Interaction Permissions
          "redshift:BatchModifyClusterIamRoles",
          "redshift:CancelQuery",
          "redshift:CancelQuerySession",
          "redshift:ConnectToCluster",
          "redshift:CreateClusterUser",
          "redshift:CreateScheduledAction",
          "redshift:DeleteScheduledAction",
          "redshift:DescribeQuery",
          "redshift:DescribeQuerySessions",
          "redshift:DescribeScheduledActions",
          "redshift:ExecuteQuery",
          "redshift:GetClusterCredentialsWithIAM",
          "redshift:ListDatabases",
          "redshift:ListQueries",
          "redshift:ListQuerySessions",
          "redshift:ListSchemas",
          "redshift:ListTables",
          "redshift:ModifyScheduledAction",
          "redshift:PauseCluster",
          "redshift:ResumeCluster",
          
          # Comprehensive Redshift Serverless Permissions
          "redshift-serverless:*"
        ]
        Resource = ["*"]
      }
    ]
  })
}

# Attach Redshift Full Access Policy
resource "aws_iam_role_policy_attachment" "attach-redshift" {
  role       = aws_iam_role.redshift-serverless-role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonRedshiftAllCommandsFullAccess"
}

resource "aws_sns_topic_policy" "schema_changes" {
  arn = var.sns_topic_arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowGluePublish"
        Effect = "Allow"
        Principal = {
          AWS = aws_iam_role.glue_service_role.arn
        }
        Action   = "SNS:Publish"
        Resource = var.sns_topic_arn
      }
    ]
  })
}



# EC2 Instance Profile
resource "aws_iam_instance_profile" "ec2_profile" {
  name = "topdevs-${var.environment}-ec2-profile"
  role = aws_iam_role.ec2_role.name
}

# Redshift Serverless Role
resource "aws_iam_role" "redshift-serverless-role" {
  name = "topdevs-${var.environment}-redshift-serverless-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Principal = {
          Service = "redshift.amazonaws.com"
        }
        Effect = "Allow"
        Sid    = ""
      },
      {
        Action = "sts:AssumeRole"
        Principal = {
          AWS = [aws_iam_role.ec2_role.arn]
        }
        Effect = "Allow"
        Sid    = ""
      }
    ]
  })

  tags = {
    Name = "topdevs-${var.environment}-redshift-serverless-role"
  }
}

# EC2 Instance Role
resource "aws_iam_role" "ec2_role" {
  name = "topdevs-${var.environment}-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name = "topdevs-${var.environment}-ec2-role"
  }
}

# Updated EC2 Policy with Redshift Serverless Query Editor permissions
resource "aws_iam_role_policy" "ec2_policy" {
  name = "topdevs-${var.environment}-ec2-policy"
  role = aws_iam_role.ec2_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Existing S3 permissions
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Resource = [
          "arn:aws:s3:::nexabrands-${var.environment}-${var.source_bucket}",
          "arn:aws:s3:::nexabrands-${var.environment}-${var.source_bucket}/*",
          "arn:aws:s3:::nexabrands-${var.environment}-${var.target_bucket}",
          "arn:aws:s3:::nexabrands-${var.environment}-${var.target_bucket}/*",
          "arn:aws:s3:::nexabrands-${var.environment}-${var.code_bucket}",
          "arn:aws:s3:::nexabrands-${var.environment}-${var.code_bucket}/*"
        ]
      },
      # Existing Glue permissions
      {
        Effect = "Allow"
        Action = [
          "glue:StartCrawler",
          "glue:StopCrawler",
          "glue:GetCrawler",
          "glue:GetCrawlers",
          "glue:GetCrawlerMetrics",
          "glue:UpdateCrawler",
          "glue:DeleteCrawler",
          "glue:CreateCrawler",
          "glue:ListCrawlers",
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun",
          "glue:GetJob",
          "glue:GetJobs",
          "glue:ListJobs",
          "glue:BatchGetJobs",
          "glue:UpdateJob",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateJob",
          "glue:DeleteJob",
          "glue:PutResourcePolicy",
          "glue:GetResourcePolicy",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:BatchGetPartition"
        ]
        Resource = ["*"]
      },
      # Expanded Redshift permissions including Query Editor full access
      {
        Effect = "Allow"
        Action = [
          # Existing Redshift permissions
          "redshift:*",
          "redshift-data:*",
          "redshift-serverless:*",
          
          # Redshift Query Editor specific permissions
          "redshift:DescribeQueryEditorV2",
          "redshift:GetQueryEditorV2Results",
          "redshift:CreateQueryEditorV2Favorites",
          "redshift:DeleteQueryEditorV2Favorites",
          "redshift:ListQueryEditorV2Favorites",
          "redshift:BatchExecuteQueryEditorQuery",
          "redshift:BatchModifyClusterIamRoles",
          "redshift:CancelQuery",
          "redshift:CancelQuerySession",
          "redshift:ConnectToCluster",
          "redshift:CreateClusterUser",
          "redshift:CreateScheduledAction",
          "redshift:DeleteScheduledAction",
          "redshift:DescribeQuery",
          "redshift:DescribeQuerySessions",
          "redshift:DescribeScheduledActions",
          "redshift:ExecuteQuery",
          "redshift:GetClusterCredentialsWithIAM",
          "redshift:ListDatabases",
          "redshift:ListQueries",
          "redshift:ListQuerySessions",
          "redshift:ListSchemas",
          "redshift:ListTables",
          "redshift:ModifyScheduledAction",
          "redshift:PauseCluster",
          "redshift:ResumeCluster",
          "redshift:UpdateQueryEditorV2Favorites"
        ]
        Resource = ["*"]
      },
      # Existing CloudWatch Logs permissions
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = ["arn:aws:logs:*:*:*"]
      },
      # Existing IAM PassRole permissions
      {
        Effect = "Allow"
        Action = "iam:PassRole"
        Resource = [
          aws_iam_role.glue_service_role.arn,
          "arn:aws:iam::*:role/topdevs-*-glue-service-role",
          "arn:aws:iam::*:role/topdevs-*-redshift-serverless-role"
        ]
        Condition = {
          StringLike = {
            "iam:PassedToService": [
              "glue.amazonaws.com",
              "redshift.amazonaws.com",
              "redshift-serverless.amazonaws.com"
            ]
          }
        }
      },
      # Existing CloudWatch Logs query permissions
      {
        Effect = "Allow"
        Action = [
          "logs:GetLogEvents",
          "logs:FilterLogEvents",
          "logs:GetLogGroupFields",
          "logs:GetQueryResults",
          "logs:StartQuery",
          "logs:StopQuery",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams"
        ]
        Resource = [
          "arn:aws:logs:us-east-1:872515289435:log-group:/aws-glue/jobs/*:*",
          "arn:aws:logs:us-east-1:872515289435:log-group:/aws-glue/jobs/output:*"
        ]
      },
      # Existing STS and AssumeRole permissions
      {
        Effect = "Allow"
        Action = [
          "sts:AssumeRole",
          "sts:GetCallerIdentity"
        ]
        Resource = [
          aws_iam_role.redshift-serverless-role.arn,
          aws_iam_role.glue_service_role.arn,
          "arn:aws:iam::872515289435:role/topdevs-*-redshift-serverless-role",
          "arn:aws:iam::872515289435:role/topdevs-*-glue-service-role"
        ]
      },
      # Existing Redshift Serverless statement permissions
      {
        Effect = "Allow"
        Action = [
          "redshift-serverless:ExecuteStatement",
          "redshift-serverless:GetStatementResult",
          "redshift-serverless:DescribeStatement",
          "redshift-serverless:CancelStatement",
          "redshift-serverless:ListStatements"
        ]
        Resource = "*"
      },
      # New KMS permissions for S3
      {
        Effect = "Allow"
        Action = [
          "kms:GenerateDataKey",
          "kms:Decrypt"
        ]
        Resource = "arn:aws:kms:us-east-1:043309357116:key/a673ca7a-2eba-4f19-bf1b-3c475c1a92e7"
      }
    ]
  })
}

# Add this new policy for Redshift KMS access
resource "aws_iam_role_policy" "redshift-kms-access-policy" {
  name = "topdevs-redshift-serverless-role-kms-policy"
  role = aws_iam_role.redshift-serverless-role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = [var.kms_key_arn]  # Make sure to pass this variable
      }
    ]
  })
}


# GitHub Actions IAM Role
resource "aws_iam_role" "github_actions_role" {
  name = "github-actions-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# IAM Policy for Redshift Access
resource "aws_iam_role_policy" "github_redshift_access" {
  name = "github-redshift-access"
  role = aws_iam_role.github_actions_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "redshift-data:ExecuteStatement",
          "redshift-data:GetStatementResult",
          "redshift-serverless:GetWorkgroup"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = [
          "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:${var.redshift_cluster_arn}-*"
        ]
      }
    ]
  })
}
