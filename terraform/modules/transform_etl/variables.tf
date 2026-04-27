
# Define variables for the transform_etl module
variable "project" {
  type        = string
  description = "Project name"
}


# Glue role ARN (passed from infra module)
variable "glue_role_arn" {
  type = string
  description = "ARN of the Glue role (passed from infra module)"
}


# Bucket Name for the data lake
variable "data_lake_bucket" {
  type        = string
  description = "Data lake bucket name"
}

# Scripts bucket name
variable "scripts_bucket_name" {
  type        = string
  description = "Glue Job scripts bucket name"
}

