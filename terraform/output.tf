
// Main output show arn and job name
output "glue_role_arn" {
  value = module.infra.glue_role_arn
}


output "glue_csv_transform_job" {
  value = module.transform_etl.glue_csv_transform_job
}


output "glue_csv_load_job" {
  value = module.load_etl.glue_csv_load_job
}
