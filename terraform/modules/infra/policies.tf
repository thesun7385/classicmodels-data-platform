# Glue base policy for the Glue job to assume the role
# Allow glue to assume the role and access necessary resources
data "aws_iam_policy_document" "glue_base_policy" {
  statement {
    sid    = "AllowGlueToAssumeRole"
    effect = "Allow"

    principals {
      identifiers = ["glue.amazonaws.com"]
      type        = "Service"
    }

    actions = ["sts:AssumeRole"]
  }
}
 
# Glue access policy for the Glue job to access necessary resources
# Allow glue to access S3, Glue, IAM, CloudWatch, SQS, EC2, RDS, and CloudTrail resources
data "aws_iam_policy_document" "glue_access_policy" {
  statement {
    sid    = "AllowGlueAccess"
    effect = "Allow"
    actions = [
      "s3:*",
      "glue:*",
      "iam:*",
      "logs:*",
      "cloudwatch:*",
      "sqs:*",
      "ec2:*",
      "rds:*",
      "cloudtrail:*"
    ]
    resources = [
      "*",
    ]
  }
} 