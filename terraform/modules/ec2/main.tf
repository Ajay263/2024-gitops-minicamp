# Modified EC2 instance
resource "aws_instance" "rag_cs_instance" {
  ami                         = var.ami_id
  instance_type              = var.instance_type
  vpc_security_group_ids     = [aws_security_group.rag_cs_sg.id]
  subnet_id                  = var.subnet_id
  iam_instance_profile       = var.ec2_instance_profile_name
  associate_public_ip_address = true
  key_name                   = aws_key_pair.ec2_key_pair.key_name
  user_data                  = base64encode(file("${path.module}/install_docker.sh"))

  tags = {
    Name = "${var.project_name}-RAG-CS-Instance"
  }
}


# Key pair for EC2 instance
resource "aws_key_pair" "ec2_key_pair" {
  key_name   = "${var.project_name}-key"
  public_key = var.public_key
}

resource "aws_security_group" "rag_cs_sg" {
  name        = "${var.project_name}-rag-cs-sg"
  description = "Security group for RAG Customer Support instance"
  vpc_id      = var.vpc_id  # Use the vpc_id from variables


  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Airflow Webserver"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Prometheus"
    from_port   = 9090
    to_port     = 9090
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Elasticsearch HTTP"
    from_port   = 9200
    to_port     = 9200
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Elasticsearch Transport"
    from_port   = 9300
    to_port     = 9300
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "PostgreSQL"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Grafana"
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "FastAPI"
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Ngrok Web Interface"
    from_port   = 4040
    to_port     = 4040
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "cAdvisor"
    from_port   = 8082
    to_port     = 8082
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
    Name = "${var.project_name}-rag-cs-sg"
  }
}



