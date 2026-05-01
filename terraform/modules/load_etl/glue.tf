
# Create glue job load
resource "aws_glue_job" "csv_load_job" {
  name         = "${var.project}-csv-load-job"
  role_arn     = var.glue_role_arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket_name}/scripts/batch_load.py"
    python_version  = 3
  }

  default_arguments = {
    "--enable-job-insights" = "true"
    "--job-language"        = "python"
    "--data_lake_bucket"    = var.data_lake_bucket
    "--presentation_prefix" = "presentation/"
  }

  # Set a timeout for the job (in minutes)
  timeout           = 5
  number_of_workers = 2
  worker_type       = "G.1X"

}
