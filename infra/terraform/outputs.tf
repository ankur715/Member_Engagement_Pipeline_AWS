output "s3_bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "redshift_copy_role_arn" {
  value = aws_iam_role.redshift_copy.arn
}

output "redshift_endpoint" {
  value = aws_redshiftserverless_workgroup.this.endpoint[0].address
}

output "redshift_port" {
  value = aws_redshiftserverless_workgroup.this.endpoint[0].port
}

output "pipeline_iam_user" {
  description = "Create an access key for this user in the IAM console, then `aws configure --profile member-engagement-pipeline`."
  value       = aws_iam_user.pipeline.name
}
