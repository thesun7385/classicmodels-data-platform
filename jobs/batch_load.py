# Todo: fix errors
# Load data for presentation layer analytics purposes
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
from pyspark.sql.functions import date_format, col # allow to reformat dates
from pyspark.sql.functions import datediff # allow to calculate difference between two dates

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

# Bucket paths
curated_bucket = f"s3://{data_lake_bucket}/curated/"
presentation_bucket = f"s3://{data_lake_bucket}/presentation/"


# Load curated tables
customers = spark.read.parquet(f"{curated_bucket}customers")
employees = spark.read.parquet(f"{curated_bucket}employees")
offices = spark.read.parquet(f"{curated_bucket}offices")
orderdetails = spark.read.parquet(f"{curated_bucket}orderdetails")
orders = spark.read.parquet(f"{curated_bucket}orders")
payments = spark.read.parquet(f"{curated_bucket}payments")
productlines = spark.read.parquet(f"{curated_bucket}productlines")
products = spark.read.parquet(f"{curated_bucket}products")


# Build presentation layer tables

# Get Sales by country
sales_by_country = payments.join(customers, "customerNumber") \
    .groupBy("country") \
    .sum("amount") \
    .withColumnRenamed("sum(amount)", "total_sales")

# Get Monthly Revenue Trend
monthly_revenue = payments \
    .withColumn("year_month", date_format(col("paymentDate"), "yyyy-MM")) \
    .groupBy("year_month") \
    .sum("amount") \
    .withColumnRenamed("sum(amount)", "monthly_revenue")

# Customer Lifetime Value
clv = payments.groupBy("customerNumber") \
    .sum('amount')\
    .withColumnRenamed("sum(amount)", "customer_lifetime_value")

# Product Performance
product_perf = orderdetails.join(products, "productCode") \
    .withColumn("revenue", col("quantityOrdered") * col("priceEach")) \
    .groupBy("productCode", "productName") \
    .sum("revenue") \
    .withColumnRenamed("sum(revenue)", "total_revenue")


# Order Fulfillment Metrics
order_fulfillment = orders.withColumn(
    "fulfillment_days",
    datediff("shippedDate", "orderDate")
)

# Write presentation layer tables
def write_table(df, name) -> (DataFrame, str):
    # write table for analytics purpose
    df.write.mode("overwrite").parquet(f"{presentation_bucket}{name}/")

# Call function to write tables
write_table(sales_by_country, "sales_by_country")
write_table(monthly_revenue, "monthly_revenue")
write_table(clv, "clv")
write_table(product_perf, "product_perf")
write_table(order_fulfillment, "order_fulfillment")

# Commit job
job.commit()