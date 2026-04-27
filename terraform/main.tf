
// Module: Infrastructure (setup IAM roles and policies, S3 buckets, etc.)
module "infra" {
    source = "./modules/infra"
    // Get variables from root module
    glue_role_name = var.glue_role_name
    data_lake_bucket= var.data_lake_bucket
}



// Module: Transformations
module "transform_etl" {
    source = "./modules/transform_etl"

    // Get variables from root module
    project =  var.project
    data_lake_bucket = var.data_lake_bucket
    scripts_bucket_name = var.scripts_bucket_name
    # Get arn from infra module and pass to transform_etl
    glue_role_arn = module.infra.glue_role_arn

}

# Exptect output
# glue_csv_transform_job = "clmodels-csv-transformation-job"
# glue_role_arn = "arn:aws:iam::637423445089:role/clmodels-glue-role"

// Todo: Module: Presentation layer - Athena, QuickSight, etc.
