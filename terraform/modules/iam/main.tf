# modules/iam/main.tf

# Glue Service Role
resource "aws_iam_role" "glue_service_role" {
  name = "topdevs-${var.environment}-glue-service-role"
  
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

# Glue Service Policy
resource "aws_iam_role_policy" "glue_service_policy" {
  name = "topdevs-${var.environment}-glue-service-policy"
  role = aws_iam_role.glue_service_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:*"
        ]
        Resource = ["*"]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation",
          "s3:ListBucket",
          "s3:GetBucketAcl"
        ]
        Resource = [
          "arn:aws:s3:::nexabrands-${var.environment}-${var.source_bucket}",
          "arn:aws:s3:::nexabrands-${var.environment}-${var.target_bucket}",
          "arn:aws:s3:::nexabrands-${var.environment}-${var.code_bucket}"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "arn:aws:s3:::nexabrands-${var.environment}-${var.source_bucket}/*",
          "arn:aws:s3:::nexabrands-${var.environment}-${var.target_bucket}/*",
          "arn:aws:s3:::nexabrands-${var.environment}-${var.code_bucket}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = [
          "arn:aws:logs:*:*:/aws-glue/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData"
        ]
        Resource = ["*"]
      },
      {
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = [var.sns_topic_arn]
      }
    ]
  })
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
          AWS = [
            aws_iam_role.ec2_role.arn
          ]
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
          "arn:aws:s3:::nexabrands-${var.environment}-${var.target_bucket}/*"
        ]
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

# EC2 Instance Profile
resource "aws_iam_instance_profile" "ec2_profile" {
  name = "topdevs-${var.environment}-ec2-profile"
  role = aws_iam_role.ec2_role.name
}

# Updated EC2 Policy with explicit STS permissions
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
          "glue:GetResourcePolicy"
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
      }
    ]
  })
}