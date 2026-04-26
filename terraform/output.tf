
// Main output show arn and job name
output "glue_role_arn" {
  value = module.infra.glue_role_arn
}


output "glue_csv_transform_job" {
  value = module.transform_etl.glue_csv_transform_job
}
