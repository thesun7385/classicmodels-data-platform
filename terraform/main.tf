
// Module: Infrastructure (setup IAM roles and policies, S3 buckets, etc.)
module "infra" {
    source = "./modules/infra"
    // Get variables from root module
    glue_role_name = var.glue_role_name 
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


// Todo: Module: Presentation layer - Athena, QuickSight, etc.