# Import sys to access command-line arguments
import sys
# Import numpy for numerical operations
import numpy as np 
import pandas as pd
from typing import Tuple

# Import AWS Glue libraries for transformations and utilities
from pyspark.context import SparkContext
from pandas import DataFrame 
from pyspark.sql.types import StructType, StructField, StringType, \
    IntegerType, DoubleType, DateType, TimestampType

# Aws Glue DynamicFrame 
from awsglue.context import GlueContext
from awsglue.transforms import * 
from awsglue.utils import getResolvedOptions 
from awsglue.job import Job 
from awsglue.dynamicframe import DynamicFrame

# Read input parameters for the job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'data_lake_bucket'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session # allow you to use read.csv, read.parquet, etc.

# Register the Glue job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Curated data lake bucket
data_lake_bucket = args['data_lake_bucket']

# Logger for the Glue job
logger = glueContext.get_logger()

# Table Schema to ensure data is read in with the correct types
tables_schema = {
    "customers": StructType([
        StructField("customerNumber", IntegerType(), True),
        StructField("customerName", StringType(), True),
        StructField("contactLastName", StringType(), True),
        StructField("contactFirstName", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("addressLine1", StringType(), True),
        StructField("addressLine2", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postalCode", StringType(), True),
        StructField("country", StringType(), True),
        StructField("salesRepEmployeeNumber", IntegerType(), True),
        StructField("creditLimit", DoubleType(), True)
    ]),
    "employees": StructType([
        StructField("employeeNumber", IntegerType(), True),
        StructField("lastName", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("extension", StringType(), True),
        StructField("email", StringType(), True),
        StructField("officeCode", StringType(), True),
        StructField("reportsTo", IntegerType(), True),
        StructField("jobTitle", StringType(), True)
    ]),
    "offices": StructType([
        StructField("officeCode", StringType(), True),
        StructField("city", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("addressLine1", StringType(), True),
        StructField("addressLine2", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("postalCode", StringType(), True),
        StructField("territory", StringType(), True)
    ]),
    "orderdetails": StructType([
        StructField("orderNumber", IntegerType(), True),
        StructField("productCode", StringType(), True),
        StructField("quantityOrdered", IntegerType(), True),
        StructField("priceEach", DoubleType(), True),
        StructField("orderLineNumber", IntegerType(), True)
    ]),
    "orders": StructType([
        StructField("orderNumber", IntegerType(), True),
        StructField("orderDate", DateType(), True),
        StructField("requiredDate", DateType(), True),
        StructField("shippedDate", DateType(), True),
        StructField("status", StringType(), True),
        StructField("comments", StringType(), True),
        StructField("customerNumber", IntegerType(), True)
    ]),
    "payments": StructType([
        StructField("customerNumber", IntegerType(), True),
        StructField("checkNumber", StringType(), True),
        StructField("paymentDate", DateType(), True),
        StructField("amount", DoubleType(), True)
    ]),
    "productlines": StructType([
        StructField("productLine", StringType(), True),
        StructField("textDescription", StringType(), True),
        StructField("htmlDescription", StringType(), True),
        StructField("image", StringType(), True)
    ]),
    "products": StructType([
        StructField("productCode", StringType(), True),
        StructField("productName", StringType(), True),
        StructField("productLine", StringType(), True),
        StructField("productScale", StringType(), True),
        StructField("productVendor", StringType(), True),
        StructField("productDescription", StringType(), True),
        StructField("quantityInStock", IntegerType(), True),
        StructField("buyPrice", DoubleType(), True),
        StructField("MSRP", DoubleType(), True)
    ])
}

# Function to add metadata columns to the DataFrame
def add_metadata(source_pd: pd.DataFrame, source_schema: StructType) -> Tuple[DataFrame, StructType]:
    
    # Add ingestion timestamp column and source column
    source_pd['ingest_ts'] = pd.Timestamp.now()
    source_pd['source'] = "classic_models_mysql"
    
    # Add the new columns to the schema
    source_schema.add(StructField("ingest_ts", TimestampType(), True))
    source_schema.add(StructField("source", StringType(), True))

    return source_pd, source_schema


# Function to enforce the schema on the DataFrame
def enforce_schema(source_pd: pd.DataFrame,
                   source_schema: StructType) -> pd.DataFrame:
    """
    Cast dataframe columns to match schema types
    """

    for field in source_schema.fields:
        field_name = field.name
        field_type = field.dataType

        # INTEGER
        if isinstance(field_type, IntegerType):
            source_pd[field_name] = (
                pd.to_numeric(source_pd[field_name], errors='coerce')
                .fillna(0) # Fill non-convertible values with 0
                .astype("int64")
            )

        # DOUBLE / FLOAT
        elif isinstance(field_type, DoubleType):
            source_pd[field_name] = (
                pd.to_numeric(source_pd[field_name], errors='coerce')
                .fillna(0.0) # Fill non-convertible values with 0.0
                .astype("float64")
            )

        # STRING
        elif isinstance(field_type, StringType):
            source_pd[field_name] = source_pd[field_name].astype(str)

        # DATE
        elif isinstance(field_type, DateType):
            source_pd[field_name] = pd.to_datetime(
                source_pd[field_name],
                errors="coerce"
            ).dt.date
            
             # convert NaT -> None explicitly
            source_pd[field_name] = source_pd[field_name].where(
                source_pd[field_name].notnull(),
                None
            )

        # TIMESTAMP (optional, future-proof)
        elif isinstance(field_type, TimestampType):
            source_pd[field_name] = pd.to_datetime(
                source_pd[field_name],
                errors="coerce"
            )

    return source_pd     


# Excecute the transformations for each table
for table_name, table_schema in tables_schema.items():
    
    # Read CSV from the landingzone
    source_data_df = spark.read \
        .option("header", "true") \
        .option("delimiter", ",") \
        .csv(f"s3://{data_lake_bucket}/landing/{table_name}.csv")
    
    # Convert to Pandas DataFrame
    source_data_pd = source_data_df.toPandas()
    
    # Call enforce_schema to cast columns to the correct data types
    source_data_pd = enforce_schema(source_data_pd, table_schema)
    logger.info(f"enforced schema done for {table_name}")
    
    # Call add_metadata to add ingestion timestamp and source columns
    target_pd, target_schema = add_metadata(source_data_pd, table_schema)
    logger.info(f"added metadata done for {table_name}")
    
    # Convert back to Pyspark
    target_data_df = spark.createDataFrame(target_pd, schema=target_schema)
    
    # Save data into the curated bucket
    target_data_path = f"s3://{data_lake_bucket}/curated/{table_name}"
    
    # Configure write
    sink = glueContext.getSink(
    connection_type="s3",
    path=target_data_path,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy"
    )
    
    # Save in parquet 
    sink.setFormat("parquet", useGlueParquetWriter=True)
    
    # Register in Glue Catalog
    sink.setCatalogInfo(
    catalogDatabase='classic_models_curated_zone',
    catalogTableName=table_name
    )
    
    # Call writeFrame
    sink.writeFrame(DynamicFrame.fromDF(target_data_df, glueContext, "target_data"))
    
    logger.info(f"write done for {table_name}")

# Commit to Glue job for transform for csv files
job.commit()
    
