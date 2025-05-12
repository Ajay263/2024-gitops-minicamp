data "aws_caller_identity" "current" {}

# EC2 Instance Role(Created first to avoid circular dependencies)
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

# Glue Service Role 
resource "aws_iam_role" "glue_service_role" {
  name = "topdevs-${var.environment}-glue-service-role"
  
  # Removed circular dependency with EC2 role
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name = "topdevs-${var.environment}-glue-service-role"
  }
}

# Redshift Serverless Role 
resource "aws_iam_role" "redshift-serverless-role" {
  name = "topdevs-${var.environment}-redshift-serverless-role"

  # Removed circular dependency with EC2 role
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
      }
    ]
  })

  tags = {
    Name = "topdevs-${var.environment}-redshift-serverless-role"
  }
}

# EC2 Instance Profile
resource "aws_iam_instance_profile" "ec2_profile" {
  name = "topdevs-${var.environment}-ec2-profile"
  role = aws_iam_role.ec2_role.name
}

# EC2 Policy 
resource "aws_iam_role_policy" "ec2_policy" {
  name = "topdevs-${var.environment}-ec2-policy"
  role = aws_iam_role.ec2_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
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
          "s3:GetReplicationConfiguration",
          "s3:GetObjectVersion",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts"
        ]
        Resource = [
          "arn:aws:s3:::nexabrand-${var.environment}-${var.source_bucket}",
          "arn:aws:s3:::nexabrand-${var.environment}-${var.source_bucket}/*",
          "arn:aws:s3:::nexabrand-${var.environment}-${var.target_bucket}",
          "arn:aws:s3:::nexabrand-${var.environment}-${var.target_bucket}/*",
          "arn:aws:s3:::nexabrand-${var.environment}-${var.code_bucket}",
          "arn:aws:s3:::nexabrand-${var.environment}-${var.code_bucket}/*",
          "arn:aws:s3:::nexabrand-${var.environment}-gx-doc",
          "arn:aws:s3:::nexabrand-${var.environment}-gx-doc/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey",
          "kms:GenerateDataKey",
          "kms:Encrypt",
          "kms:ReEncrypt*",
          "kms:ListKeys",
          "kms:ListAliases"
        ]
        Resource = [
          "*"
        ]
      },
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
      {
        Effect = "Allow"
        Action = [
          "redshift:*",
          "redshift-data:*",
          "redshift-serverless:*"
        ]
        Resource = ["*"]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = ["arn:aws:logs:*:*:*"]
      },
      {
        Effect = "Allow"
        Action = "iam:PassRole"
        Resource = [
          "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/topdevs-${var.environment}-glue-service-role",
          "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/topdevs-${var.environment}-redshift-serverless-role",
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
          "arn:aws:logs:us-east-1:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/jobs/*:*",
          "arn:aws:logs:us-east-1:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/jobs/output:*"
        ]
      },
      # STS and AssumeRole permissions(Using interpolated ARNs)
      {
        Effect = "Allow"
        Action = [
          "sts:AssumeRole",
          "sts:GetCallerIdentity"
        ]
        Resource = [
          "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/topdevs-${var.environment}-redshift-serverless-role",
          "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/topdevs-${var.environment}-glue-service-role",
          "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/topdevs-*-redshift-serverless-role",
          "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/topdevs-*-glue-service-role"
        ]
      },
      # Redshift Serverless statement permissions
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

# Glue Service Role Policy
resource "aws_iam_role_policy" "glue_service_policy" {
  name = "topdevs-${var.environment}-glue-service-policy"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["glue:*"]
        Resource = ["*"]
      },
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
          "arn:aws:s3:::nexabrand-${var.environment}-${var.code_bucket}/*",
          "arn:aws:s3:::nexabrand-${var.environment}-gx-doc",
          "arn:aws:s3:::nexabrand-${var.environment}-gx-doc/*"
        ]
      },
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

  depends_on = [
    aws_iam_role.glue_service_role
  ]
}
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
          "arn:aws:s3:::nexabrand-${var.environment}-${var.code_bucket}/*",
          "arn:aws:s3:::nexabrand-${var.environment}-gx-doc",
          "arn:aws:s3:::nexabrand-${var.environment}-gx-doc/*"
        ]
      }
    ]
  })

  depends_on = [
    aws_iam_role.redshift-serverless-role
  ]
}

# Redshift Glue Access Policy
resource "aws_iam_role_policy" "redshift-glue-access-policy" {
  name = "topdevs-${var.environment}-redshift-serverless-role-glue-policy"
  role = aws_iam_role.redshift-serverless-role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
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
   
      {
        Effect = "Allow"
        Action = [
          "redshift:DescribeQueryEditorV2",
          "redshift:GetQueryEditorV2Results",
          "redshift:CreateQueryEditorV2Favorites",
          "redshift:DeleteQueryEditorV2Favorites",
          "redshift:ListQueryEditorV2Favorites",
          "redshift:BatchExecuteQueryEditorQuery",
          "redshift:UpdateQueryEditorV2Favorites",
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
          "redshift-serverless:*"
        ]
        Resource = ["*"]
      }
    ]
  })

  depends_on = [
    aws_iam_role.redshift-serverless-role
  ]
}

# Attach Redshift Full Access Policy
resource "aws_iam_role_policy_attachment" "attach-redshift" {
  role       = aws_iam_role.redshift-serverless-role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonRedshiftAllCommandsFullAccess"
  
  depends_on = [
    aws_iam_role.redshift-serverless-role
  ]
}

# resource "aws_sns_topic_policy" "schema_changes" {
#   arn = var.sns_topic_arn
#   policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [
#       {
#         Sid    = "AllowGluePublish"
#         Effect = "Allow"
#         Principal = {
#           AWS = aws_iam_role.glue_service_role.arn
#         }
#         Action   = "SNS:Publish"
#         Resource = var.sns_topic_arn
#       }
#     ]
#   })
  
#   depends_on = [
#     aws_iam_role.glue_service_role
#   ]
# }

# New S3 bucket for hosting static website (Great Expectations documentation)
resource "aws_s3_bucket" "gx_doc" {
  bucket        = "nexabrand-${var.environment}-gx-doc"
  force_destroy = true
}

# Set bucket ownership controls
resource "aws_s3_bucket_ownership_controls" "gx_doc_ownership" {
  bucket = aws_s3_bucket.gx_doc.id
  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

# Configure public access settings
resource "aws_s3_bucket_public_access_block" "gx_doc_public_access" {
  bucket = aws_s3_bucket.gx_doc.id
  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}

# Set bucket ACL to public-read
resource "aws_s3_bucket_acl" "gx_doc_acl" {
  depends_on = [
    aws_s3_bucket_ownership_controls.gx_doc_ownership,
    aws_s3_bucket_public_access_block.gx_doc_public_access,
  ]
  
  bucket = aws_s3_bucket.gx_doc.id
  acl    = "public-read"
}

# Enable bucket versioning
resource "aws_s3_bucket_versioning" "gx_doc_versioning" {
  bucket = aws_s3_bucket.gx_doc.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Configure website hosting
resource "aws_s3_bucket_website_configuration" "gx_doc_website" {
  bucket = aws_s3_bucket.gx_doc.id
  
  index_document {
    suffix = "index.html"
  }
  
  error_document {
    key = "error.html"
  }
}

# Add bucket policy to allow public read access
resource "aws_s3_bucket_policy" "gx_doc_policy" {
  depends_on = [
    aws_s3_bucket_public_access_block.gx_doc_public_access
  ]
    
  bucket = aws_s3_bucket.gx_doc.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = "*"
        Action    = [
          "s3:GetObject"
        ]
        Resource = [
          "${aws_s3_bucket.gx_doc.arn}",
          "${aws_s3_bucket.gx_doc.arn}/*"
        ]
      }
    ]
  })
}