
# Output the Glue role ARN and Glue job name for use in other modules or for reference

output "glue_csv_transform_job" {
  value = aws_glue_job.csv_transformation_job.name
}
