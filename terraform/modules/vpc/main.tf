data "aws_availability_zones" "available" {}

resource "aws_vpc" "redshift-serverless-vpc" {
  cidr_block           = var.redshift_serverless_vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "${var.app_name}-redshift-serverless-vpc"
  }
}

# Public Subnets in All AZs
resource "aws_subnet" "public_subnet_az1" {
  vpc_id                  = aws_vpc.redshift-serverless-vpc.id
  cidr_block              = "10.0.10.0/24"
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true
  tags = { Name = "${var.app_name}-public-subnet-az1" }
}

resource "aws_subnet" "public_subnet_az2" {
  vpc_id                  = aws_vpc.redshift-serverless-vpc.id
  cidr_block              = "10.0.11.0/24"
  availability_zone       = data.aws_availability_zones.available.names[1]
  map_public_ip_on_launch = true
  tags = { Name = "${var.app_name}-public-subnet-az2" }
}

resource "aws_subnet" "public_subnet_az3" {
  vpc_id                  = aws_vpc.redshift-serverless-vpc.id
  cidr_block              = "10.0.12.0/24"
  availability_zone       = data.aws_availability_zones.available.names[2]
  map_public_ip_on_launch = true
  tags = { Name = "${var.app_name}-public-subnet-az3" }
}

# Internet Gateway
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.redshift-serverless-vpc.id
  tags = { Name = "${var.app_name}-igw" }
}

# Public Route Table
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.redshift-serverless-vpc.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }
  tags = { Name = "${var.app_name}-public-rt" }
}

# Associate All Public Subnets
resource "aws_route_table_association" "public_az1" {
  subnet_id      = aws_subnet.public_subnet_az1.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "public_az2" {
  subnet_id      = aws_subnet.public_subnet_az2.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "public_az3" {
  subnet_id      = aws_subnet.public_subnet_az3.id
  route_table_id = aws_route_table.public.id
}

# Private Subnets (Original Configuration)
resource "aws_subnet" "redshift-serverless-subnet-az1" {
  vpc_id            = aws_vpc.redshift-serverless-vpc.id
  cidr_block        = var.redshift_serverless_subnet_1_cidr
  availability_zone = data.aws_availability_zones.available.names[0]
  tags = { Name = "${var.app_name}-redshift-serverless-subnet-az1" }
}

resource "aws_subnet" "redshift-serverless-subnet-az2" {
  vpc_id            = aws_vpc.redshift-serverless-vpc.id
  cidr_block        = var.redshift_serverless_subnet_2_cidr
  availability_zone = data.aws_availability_zones.available.names[1]
  tags = { Name = "${var.app_name}-redshift-serverless-subnet-az2" }
}

resource "aws_subnet" "redshift-serverless-subnet-az3" {
  vpc_id            = aws_vpc.redshift-serverless-vpc.id
  cidr_block        = var.redshift_serverless_subnet_3_cidr
  availability_zone = data.aws_availability_zones.available.names[2]
  tags = { Name = "${var.app_name}-redshift-serverless-subnet-az3" }
}

# Security Group with Open Redshift Port
# modules/vpc/main.tf
resource "aws_security_group" "redshift-serverless-security-group" {
  name        = "${var.app_name}-redshift-serverless-security-group"
  description = "Security group for Redshift Serverless"
  vpc_id      = aws_vpc.redshift-serverless-vpc.id

  ingress {
    description = "Redshift port access"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # Open to all for GitHub runners
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
# NAT Gateway Configuration (Original)
resource "aws_eip" "nat" {
  domain = "vpc"
  tags = { Name = "${var.app_name}-nat-eip" }
}

resource "aws_nat_gateway" "main" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public_subnet_az1.id
  tags = { Name = "${var.app_name}-nat-gateway" }
}

# Private Route Table
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.redshift-serverless-vpc.id
  route {
    cidr_block = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.main.id
  }
  tags = { Name = "${var.app_name}-private-rt" }
}

# Associate Private Subnets
resource "aws_route_table_association" "private_az1" {
  subnet_id      = aws_subnet.redshift-serverless-subnet-az1.id
  route_table_id = aws_route_table.private.id
}

resource "aws_route_table_association" "private_az2" {
  subnet_id      = aws_subnet.redshift-serverless-subnet-az2.id
  route_table_id = aws_route_table.private.id
}

resource "aws_route_table_association" "private_az3" {
  subnet_id      = aws_subnet.redshift-serverless-subnet-az3.id
  route_table_id = aws_route_table.private.id
}

# VPC Endpoint for S3 (Original)
resource "aws_vpc_endpoint" "s3_redshift" {
  vpc_id            = aws_vpc.redshift-serverless-vpc.id
  service_name      = "com.amazonaws.${data.aws_region.current.name}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_vpc.redshift-serverless-vpc.default_route_table_id]
  tags = { Name = "${var.app_name}-s3-endpoint" }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}