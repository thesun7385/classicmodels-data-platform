
# Get the ARN of the Glue role
output "glue_role_arn" {
  value = aws_iam_role.glue_job_role.arn
}
