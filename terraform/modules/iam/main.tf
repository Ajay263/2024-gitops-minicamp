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
          "arn:aws:s3:::nexabrand-${var.environment}-${var.source_bucket}",
          "arn:aws:s3:::nexabrand-${var.environment}-${var.source_bucket}/*",
          "arn:aws:s3:::nexabrand-${var.environment}-${var.target_bucket}",
          "arn:aws:s3:::nexabrand-${var.environment}-${var.target_bucket}/*",
          "arn:aws:s3:::nexabrand-${var.environment}-${var.code_bucket}",
          "arn:aws:s3:::nexabrand-${var.environment}-${var.code_bucket}/*"
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
          "arn:aws:s3:::nexabrand-${var.environment}-${var.source_bucket}",
          "arn:aws:s3:::nexabrand-${var.environment}-${var.source_bucket}/*",
          "arn:aws:s3:::nexabrand-${var.environment}-${var.target_bucket}",
          "arn:aws:s3:::nexabrand-${var.environment}-${var.target_bucket}/*",
          "arn:aws:s3:::nexabrand-${var.environment}-${var.code_bucket}",
          "arn:aws:s3:::nexabrand-${var.environment}-${var.code_bucket}/*"
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

resource "aws_iam_role_policy" "ec2_policy" {
  name = "topdevs-${var.environment}-ec2-policy"
  role = aws_iam_role.ec2_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # S3 permissions including the new GX docs bucket
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:GetBucketLocation", 
          "s3:DeleteObject",
          "s3:GetBucketAcl",       
          "s3:PutObjectAcl",
          # Additional S3 admin actions for full control
          "s3:CreateBucket",
          "s3:DeleteBucket",
          "s3:PutBucketPolicy",
          "s3:GetBucketPolicy",
          "s3:PutBucketAcl",
          "s3:PutBucketWebsite",
          "s3:GetBucketWebsite",
          "s3:PutEncryptionConfiguration",
          "s3:GetEncryptionConfiguration",
          "s3:PutLifecycleConfiguration",
          "s3:GetLifecycleConfiguration",
          "s3:PutBucketVersioning",
          "s3:GetBucketVersioning",
          "s3:ListBucketVersions",
          "s3:PutReplicationConfiguration",
          "s3:GetReplicationConfiguration"
        ]
        Resource = [
          "arn:aws:s3:::nexabrand-${var.environment}-${var.source_bucket}",
          "arn:aws:s3:::nexabrand-${var.environment}-${var.source_bucket}/*",
          "arn:aws:s3:::nexabrand-${var.environment}-${var.target_bucket}",
          "arn:aws:s3:::nexabrand-${var.environment}-${var.target_bucket}/*",
          "arn:aws:s3:::nexabrand-${var.environment}-${var.code_bucket}",
          "arn:aws:s3:::nexabrand-${var.environment}-${var.code_bucket}/*",
    
        ]
      },
      # KMS permissions for EC2 (unchanged)
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey",
          "kms:GenerateDataKey",
          "kms:Encrypt"
        ]
        Resource = [
          "*"
        ]
      },
      # Glue permissions (unchanged)
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
      # Redshift permissions (unchanged)
      {
        Effect = "Allow"
        Action = [
          "redshift:*",
          "redshift-data:*",
          "redshift-serverless:*",
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
      # CloudWatch Logs permissions (unchanged)
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = ["arn:aws:logs:*:*:*"]
      },
      # IAM PassRole permissions (unchanged)
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
      # CloudWatch Logs query permissions (unchanged)
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
      # STS and AssumeRole permissions (unchanged)
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
      # Redshift Serverless statement permissions (unchanged)
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
      }
    ]
  })
}