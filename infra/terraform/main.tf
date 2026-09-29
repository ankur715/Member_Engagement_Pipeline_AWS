terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
      DataClass = "synthetic-phi" # tag every resource so a real-PHI env is never confused with this one
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  # Account id suffix keeps the bucket name globally unique without a random provider.
  bucket_name = "${var.project}-lake-${data.aws_caller_identity.current.account_id}"
}

# ---------------------------------------------------------------------------
# S3 data lake: raw/ (health-plan files + API payloads, as received),
# staged/ (Parquet for COPY), rejects/ (rows that failed validation),
# exports/ (reports sent to customers).
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket_name
  force_destroy = true # synthetic data only -- lets `terraform destroy` clean up fully
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# SSE-S3 rather than a customer-managed KMS key: free, and still encrypted at
# rest. A production PHI bucket would use SSE-KMS for key-level audit trails.
resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket     = aws_s3_bucket.lake.id
  depends_on = [aws_s3_bucket_versioning.lake]

  rule {
    id     = "expire-staged-parquet"
    status = "Enabled"
    filter {
      prefix = "staged/"
    }
    expiration {
      days = 14 # staged Parquet is reproducible from raw/, no reason to keep it
    }
  }

  rule {
    id     = "expire-raw"
    status = "Enabled"
    filter {
      prefix = "raw/"
    }
    expiration {
      days = var.raw_retention_days
    }
  }

  rule {
    id     = "expire-old-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }
}

# Deny any request that isn't over TLS -- encryption in transit for PHI.
resource "aws_s3_bucket_policy" "lake" {
  bucket     = aws_s3_bucket.lake.id
  depends_on = [aws_s3_bucket_public_access_block.lake]
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

# ---------------------------------------------------------------------------
# IAM: least privilege, one identity per job.
#   - redshift_copy: assumed by Redshift to COPY from staged/ only
#   - pipeline user: what Airflow/Python runs as (read/write raw, staged, rejects, exports)
# ---------------------------------------------------------------------------

resource "aws_iam_role" "redshift_copy" {
  name = "${var.project}-redshift-copy"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = ["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"]
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "redshift_copy" {
  name = "read-staged-parquet"
  role = aws_iam_role.redshift_copy.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${aws_s3_bucket.lake.arn}/staged/*" # not raw/: Redshift never sees source documents
      },
      {
        Effect    = "Allow"
        Action    = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource  = aws_s3_bucket.lake.arn
        Condition = { StringLike = { "s3:prefix" = ["staged/*"] } }
      }
    ]
  })
}

resource "aws_iam_user" "pipeline" {
  name = "${var.project}-pipeline"
}

resource "aws_iam_user_policy" "pipeline" {
  name = "pipeline-s3-access"
  user = aws_iam_user.pipeline.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject"]
        Resource = [
          "${aws_s3_bucket.lake.arn}/raw/*",
          "${aws_s3_bucket.lake.arn}/staged/*",
          "${aws_s3_bucket.lake.arn}/rejects/*",
          "${aws_s3_bucket.lake.arn}/exports/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.lake.arn
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Redshift Serverless -- bills per RPU-second only while queries run, and
# pauses when idle. The usage limit below is the hard cost stop.
# ---------------------------------------------------------------------------

data "aws_vpc" "default" {
  default = true
}

# Redshift Serverless needs subnets in >= 3 AZs and doesn't support every AZ
# (e.g. us-east-1e), so filter explicitly.
data "aws_subnets" "redshift" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "availability-zone"
    values = var.redshift_azs
  }
}

resource "aws_security_group" "redshift" {
  name        = "${var.project}-redshift"
  description = "Redshift Serverless access from the developer IP only"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "Redshift from my IP"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_redshiftserverless_namespace" "this" {
  namespace_name       = var.project
  db_name              = "engagement"
  admin_username       = var.redshift_admin_username
  admin_user_password  = var.redshift_admin_password
  iam_roles            = [aws_iam_role.redshift_copy.arn]
  default_iam_role_arn = aws_iam_role.redshift_copy.arn
}

resource "aws_redshiftserverless_workgroup" "this" {
  namespace_name = aws_redshiftserverless_namespace.this.namespace_name
  workgroup_name = var.project
  base_capacity  = var.redshift_base_rpu
  # Public endpoint so a laptop-hosted Airflow can reach it; the security
  # group above limits it to one /32. A real deployment keeps it private
  # and runs the orchestrator inside the VPC.
  publicly_accessible = true
  subnet_ids          = data.aws_subnets.redshift.ids
  security_group_ids  = [aws_security_group.redshift.id]
}

resource "aws_redshiftserverless_usage_limit" "daily_compute" {
  resource_arn  = aws_redshiftserverless_workgroup.this.arn
  usage_type    = "serverless-compute"
  amount        = var.redshift_daily_rpu_hours
  period        = "daily"
  breach_action = "deactivate" # stop serving queries rather than keep billing
}

# ---------------------------------------------------------------------------
# Budget alert (the first two AWS Budgets are free).
# ---------------------------------------------------------------------------

resource "aws_budgets_budget" "monthly" {
  name         = "${var.project}-monthly"
  budget_type  = "COST"
  limit_amount = var.monthly_budget_usd
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 50
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}
