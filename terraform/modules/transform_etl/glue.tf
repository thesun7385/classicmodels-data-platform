
# Create glue job for transform
resource "aws_glue_job" "csv_transformation_job" {
    name         = "${var.project}-csv-transformation-job"
    role_arn     = var.glue_role_arn
    glue_version = "4.0"

    command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket_name}/scripts/batch_transform.py"
    python_version  = 3
  }

    default_arguments = {
    "--enable-job-insights" = "true"
    "--job-language"        = "python"
    "--data_lake_bucket" = var.data_lake_bucket
    "--curated_prefix"   = "curated/"
  }

  # Set a timeout for the job (in minutes)
  timeout = 5
  number_of_workers = 2
  worker_type       = "G.1X"

}