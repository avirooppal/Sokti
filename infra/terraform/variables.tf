variable "aws_region" {
  type        = string
  default     = "ap-south-1"
  description = "AWS region for deployment"
}

variable "environment" {
  type        = string
  default     = "production"
  description = "Deployment environment name"
}

variable "db_password" {
  type        = string
  sensitive   = true
  description = "Master password for PostgreSQL RDS"
}
