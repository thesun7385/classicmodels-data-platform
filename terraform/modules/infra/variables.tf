
# Glue role name variable for the Glue job to assume the role
variable "glue_role_name" {
  type        = string
  description = "Role to be used for glue jobs"
}

# Bucket Name for the data lake
variable "data_lake_bucket" {
  type        = string
  description = "Data lake bucket name"
}