# Import os when we need to read files from a directory
import os
# Import pandas to create a DataFrame and save it as a CSV file
import pandas as pd
# Import create_engine from SQLAlchemy to connect to the database
from sqlalchemy import create_engine
from dotenv import load_dotenv
import csv
# Import boto for AWS services
import boto3

# Load environment variables from .env file
load_dotenv()

# Create a connection to the database using SQLAlchemy
engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

# Check if the connection is successful
try:
    with engine.connect() as connection:
        print("Connection to the database was successful!")
except Exception as e:
    print(f"An error occurred while connecting to the database: {e}")
    exit(1)
    
# Output directory for CSV files
OUTPUT_DIR = "../data/seed"

# Create the output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

    
# Get table names from the database
TABLE_NAME = [
    "customers" ,
    "employees" ,
    "offices" ,
    "orderdetails" ,
    "orders" ,
    "payments" ,
    "productlines" ,
    "products"]

# Function to extract data from a table and save it as a CSV file
def extract_table_to_csv(table_name):
    try:
        # SQL Query
        query = f"SELECT * FROM {table_name}"
        
        # Read the table into a DataFrame
        df = pd.read_sql(query, engine)
        
        # Save the data frame to a CSV file
        file_path = os.path.join(OUTPUT_DIR, f"{table_name}.csv")
        
        # Save the DataFrame to a CSV file without the index
        df.to_csv(
            file_path,
            index=False,
            sep=',',
        )
        
        print(f"Data from table '{table_name}' has been successfully saved to '{file_path}'")
    except Exception as e:
        print(f"An error occurred while extracting data from table '{table_name}': {e}")
        
# Function to upload a file to S3        
def upload_to_s3(file_path, bucket_name, object_name):
    # Create an S3 client
    s3_client = boto3.client('s3')
    
    try:
        # Upload the file to S3
        s3_client.upload_file(
        file_path,
        bucket_name,
        object_name,
        ExtraArgs={"ContentType": "text/csv"} # Specify csv
        )
        print(f"File '{file_path}' has been successfully uploaded to S3 bucket '{bucket_name}' as '{object_name}'")
    except Exception as e:
        print(f"An error occurred while uploading file '{file_path}' to S3: {e}")
        
        

# Main
def main():
    # Loop through each table name and extract data to CSV
    for table in TABLE_NAME:
        # Extract data from the table and save it as a CSV file
        extract_table_to_csv(table)
        
    # Upload the CSV files to S3
    for table in TABLE_NAME:
        
        # Get the file path for the CSV file
        file_path = os.path.join(OUTPUT_DIR, f"{table}.csv")
        
        # Get the bucket name 
        bucket_name = os.getenv("S3_BUCKET_NAME")
        
        # S3 object key (specifies the path in the bucket where the file will be stored)
        object_name = f"landing/{table}.csv"
        
        # Upload the file to S3
        upload_to_s3(file_path, bucket_name, object_name)

# This means that if this script is run directly (instead of imported as a module), the main() function will be executed
if __name__ == "__main__":
    main()