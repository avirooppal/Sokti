terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ==============================================================================
# 1. VPC & Networking
# ==============================================================================
resource "aws_vpc" "sokti_vpc" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags = {
    Name        = "sokti-data-platform-vpc"
    Environment = var.environment
  }
}

resource "aws_subnet" "public_1" {
  vpc_id            = aws_vpc.sokti_vpc.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = "${var.aws_region}a"
  tags = { Name = "sokti-public-subnet-1" }
}

resource "aws_subnet" "private_1" {
  vpc_id            = aws_vpc.sokti_vpc.id
  cidr_block        = "10.0.10.0/24"
  availability_zone = "${var.aws_region}a"
  tags = { Name = "sokti-private-subnet-1" }
}

# ==============================================================================
# 2. S3 Lakehouse Buckets
# ==============================================================================
resource "aws_s3_bucket" "lakehouse_raw" {
  bucket        = "sokti-${var.environment}-lakehouse-raw"
  force_destroy = false
  tags = {
    DataClassification = "Internal"
    Tier               = "Bronze"
  }
}

resource "aws_s3_bucket" "lakehouse_curated" {
  bucket        = "sokti-${var.environment}-lakehouse-iceberg"
  force_destroy = false
  tags = {
    DataClassification = "Aggregated"
    Tier               = "Silver-Gold"
  }
}

# ==============================================================================
# 3. Amazon Managed Streaming for Apache Kafka (MSK)
# ==============================================================================
resource "aws_msk_cluster" "sokti_kafka" {
  cluster_name           = "sokti-events-msk"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = [aws_subnet.private_1.id]
    security_groups = [aws_security_group.kafka_sg.id]
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  tags = {
    Platform = "Sokti"
  }
}

resource "aws_security_group" "kafka_sg" {
  name        = "sokti-msk-sg"
  description = "Security group for MSK Kafka cluster"
  vpc_id      = aws_vpc.sokti_vpc.id

  ingress {
    from_port   = 9092
    to_port     = 9094
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ==============================================================================
# 4. Amazon RDS PostgreSQL (OLTP & pgvector)
# ==============================================================================
resource "aws_db_instance" "oltp_postgres" {
  identifier          = "sokti-oltp-postgres"
  allocated_storage   = 100
  engine              = "postgres"
  engine_version      = "16.1"
  instance_class      = "db.r6g.xlarge"
  username            = "sokti_admin"
  password            = var.db_password
  skip_final_snapshot = true
  vpc_security_group_ids = [aws_security_group.postgres_sg.id]
  db_subnet_group_name   = "sokti-db-subnet-group"
}

resource "aws_security_group" "postgres_sg" {
  name        = "sokti-postgres-sg"
  vpc_id      = aws_vpc.sokti_vpc.id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }
}
