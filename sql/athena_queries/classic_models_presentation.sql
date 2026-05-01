-- Create classic_models_presentation
CREATE DATABASE classic_models_presentation;

-- Create sales_by_country table
CREATE EXTERNAL TABLE classic_models_presentation.sales_by_country (
    country STRING,
    total_sales DOUBLE
)
STORED AS PARQUET
LOCATION 's3://{S3_BUCKET_NAME}/presentation/sales_by_country/';


-- Create customer lifetime value TABLE
CREATE EXTERNAL TABLE classic_models_presentation.clv (
    customerNumber INT,
    customer_lifetime_value DOUBLE
)
STORED AS PARQUET
LOCATION 's3://{S3_BUCKET_NAME}/presentation/clv/';

Create product_perforamce TABLE
CREATE EXTERNAL TABLE classic_models_presentation.product_perf (
    productCode STRING,
    productName STRING,
    total_revenue DOUBLE
)
STORED AS PARQUET
LOCATION 's3://{S3_BUCKET_NAME}/presentation/product_perf/';

-- Create order_fullfillment table
CREATE EXTERNAL TABLE classic_models_presentation.order_fulfillment (
    orderNumber INT,
    orderDate DATE,
    requiredDate DATE,
    shippedDate DATE,
    status STRING,
    comments STRING,
    customerNumber INT,
    fulfillment_days INT
)
STORED AS PARQUET
LOCATION 's3://{S3_BUCKET_NAME}/presentation/order_fulfillment/';


