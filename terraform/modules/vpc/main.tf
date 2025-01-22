# modules/vpc/main.tf
data "aws_availability_zones" "available" {}

resource "aws_vpc" "redshift-serverless-vpc" {
  cidr_block           = var.redshift_serverless_vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "${var.app_name}-redshift-serverless-vpc"
  }
}

# Public Subnet
resource "aws_subnet" "public_subnet_az1" {
  vpc_id                  = aws_vpc.redshift-serverless-vpc.id
  cidr_block              = "10.0.10.0/24"  # Adjust CIDR as needed
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.app_name}-public-subnet-az1"
  }
}

# Internet Gateway
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.redshift-serverless-vpc.id

  tags = {
    Name = "${var.app_name}-igw"
  }
}

# Route Table for Public Subnet
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.redshift-serverless-vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "${var.app_name}-public-rt"
  }
}

# Associate public subnet with public route table
resource "aws_route_table_association" "public_az1" {
  subnet_id      = aws_subnet.public_subnet_az1.id
  route_table_id = aws_route_table.public.id
}

# Private subnets for Redshift
resource "aws_subnet" "redshift-serverless-subnet-az1" {
  vpc_id            = aws_vpc.redshift-serverless-vpc.id
  cidr_block        = var.redshift_serverless_subnet_1_cidr
  availability_zone = data.aws_availability_zones.available.names[0]

  tags = {
    Name = "${var.app_name}-redshift-serverless-subnet-az1"
  }
}

resource "aws_subnet" "redshift-serverless-subnet-az2" {
  vpc_id            = aws_vpc.redshift-serverless-vpc.id
  cidr_block        = var.redshift_serverless_subnet_2_cidr
  availability_zone = data.aws_availability_zones.available.names[1]

  tags = {
    Name = "${var.app_name}-redshift-serverless-subnet-az2"
  }
}

resource "aws_subnet" "redshift-serverless-subnet-az3" {
  vpc_id            = aws_vpc.redshift-serverless-vpc.id
  cidr_block        = var.redshift_serverless_subnet_3_cidr
  availability_zone = data.aws_availability_zones.available.names[2]

  tags = {
    Name = "${var.app_name}-redshift-serverless-subnet-az3"
  }
}

resource "aws_security_group" "redshift-serverless-security-group" {
  name        = "${var.app_name}-redshift-serverless-security-group"
  description = "${var.app_name}-redshift-serverless-security-group"
  vpc_id      = aws_vpc.redshift-serverless-vpc.id

  ingress {
    description = "all traffic"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.app_name}-redshift-serverless-security-group"
  }
}

resource "aws_vpc_endpoint" "s3_redshift" {
  vpc_id            = aws_vpc.redshift-serverless-vpc.id
  service_name      = "com.amazonaws.us-east-1.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_vpc.redshift-serverless-vpc.default_route_table_id]

  tags = {
    Name = "${var.app_name}-s3-endpoint"
  }
}
