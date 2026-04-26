# IAM role for Glue jobs
resource "aws_iam_role" "glue_job_role" {
  name = var.glue_role_name

  # Allow Glue execute the jobs with the permissions defined in the attached policies
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}
