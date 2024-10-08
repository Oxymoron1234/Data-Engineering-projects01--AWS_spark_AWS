import os, csv


key = "sales_project"
iv = "sales_encyptyo"
salt = "sales_AesEncryption"

#AWS Access And Secret key
access_key_path ="/Users/ashishkumarjha/Desktop/Data Engg/Sales_project_01/spark_ec2_project_01_accessKeys.csv"
with open(access_key_path , mode='r', newline='') as csvfile:
    reader = csv.reader(csvfile)

    # Optionally, you can skip the header
    header = next(reader)

    for row in reader:
        aws_access_key = row[0]
        aws_secret_key = row[1]
bucket_name = "de-sales-project-01"
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_source_directory = "sales_data/"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"
#print(aws_access_key , aws_secret_key)

#Database credential
# MySQL database connection properties
host_name = "localhost"
database_name = "sales_project"
url = f"jdbc:mysql://localhost:3306/{database_name}"
properties = {
    "user": "root",
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver",

}

# Table name
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Required columns
mandatory_columns = ["customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost"]

mandatory_columns_type = ["int","int","varchar(100)","date","int","decimal(10,2)","int","decimal(10,2)"]

# File Download location
base_location = "/Users/ashishkumarjha/Desktop/Data Engg/Sales_project_01/"
local_directory = base_location + "file_from_s3/"
customer_data_mart_local_file = base_location + "customer_data_mart/"
sales_team_data_mart_local_file = base_location + "sales_team_data_mart/"
sales_team_data_mart_partitioned_local_file = base_location + "sales_partition_data/"
error_folder_path_local = base_location + "error_files/"
