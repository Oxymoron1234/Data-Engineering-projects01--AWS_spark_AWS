import os
import shutil
import sys
from datetime import datetime

from pyspark.sql.functions import concat_ws, lit, expr, substring, to_date, date_format
from pyspark.sql.types import IntegerType, StructField, StringType, StructType, DateType, FloatType

from src.main.delete.local_file_delete import delete_local_file
from src.main.download.aws_file_download import *
from src.main.move.move_files import move_s3_to_s3
from src.main.read.aws_read import S3Reader
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.dimension_tables_join import dimesions_table_join
from src.main.transformations.jobs.sales_mart_sql_transformation_write import  sales_mart_calculation_table_write
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import *
from src.main.utility.logging_config import logger
from src.main.utility.my_sql_session import *
from src.main.utility.s3_client_object import *
from src.main.utility.spark_session import spark_session
from src.main.write.parquet_writer import GeneralWriter

###############getting s3 client#####

s3_client_provider = S3ClientProvider(decrypt_data(encrypted_aws_access_key), decrypt_data(encrypted_aws_secret_key))
print(s3_client_provider.get_client())
s3_client = s3_client_provider.get_client()


#Now we can use s3_client for our s3 operations
responses = s3_client.list_buckets()
logger.info(f"list of buckets {responses['Buckets']}")

#chek if local directory has already a file
#if file is there then check if the same file is present in the staging area
#with a status as A. if so then dont delecte and try to rerun
#else give an error and not proceed the next file

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)
    statement = f""" 
    select distinct file_name from 
    {config.database_name}.{config.product_staging_table}
    where file_name in ({str(total_csv_files)[1:-1]}) and status = 'A'
    """

    logger.info(f"dynamically statement created: {statement}")
    cursor.execute(statement)
    data = cursor.fetchall()
    if data:
        logger.info("Your last run was failed please check")
    else:
        logger.info("No record found !!!")
else:
    logger.info("Last run was successful !!")
try:
    s3_reader = S3Reader()
    #Bucket name should come from table
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client,config.bucket_name, folder_path = folder_path)
    logger.info(f"Absolute path on s3 bucket for csv file {s3_absolute_file_path}")
    if not s3_absolute_file_path:
        logger.info(f"No files available to process")
        raise Exception("No Data available to process")
except Exception as e:
    logger.error(f"Exit with error {e}")
    raise e

prefix = f"s3://{bucket_name}"
file_path = [url[len(prefix):] for url in s3_absolute_file_path]
logger.info(f"File path available on s3 under {bucket_name} bucket and folder name is {file_path}")
try:
    downloader = S3FileDownloader(s3_client,bucket_name,local_directory)
    downloader.download_files(file_path)
except Exception as e:
    logger.error(f"File download error {e}")
    sys.exit()

#Get a list of all the files in the local directory
all_files = os.listdir(local_directory)
logger.info(f"List of files present at my local directory after download {all_files}")

#Filter files with .csv in their name and create absolute paths
if all_files:
    csv_files = []
    error_files = []
    for files in all_files:
        if files.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(local_directory,files)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory,files)))
    if not csv_files:
        logger.error("No csv data available to process the request")
        raise Exception("No csv data available to process the request")
else:
    logger.error("there is no data to process")
    raise Exception("there is no data to process")

# Example usage
#check_object_exists(s3_client, bucket_name, 'sales_data/Drug_clean.csv')

#################make csv lines convert into a list of comma separated ####

#csv_files = str(csv_files)[1:-1]
logger.info("***********list of files******")
logger.info(f"list of csv files that need to be processed {csv_files}")

logger.info("*********creating spark session ******")
spark = spark_session()
logger.info("*********** spark session created ********")

#check the requirement column in the schema of csv files
#if not required columns keep it in a list or error_files
#else union all the data into one data frame

logger.info("*********** Checking Schema for data loaded in s3 *********")
correct_files = []
for data in csv_files:
    data_schema = spark.read.format("csv")\
        .option("header","true")\
        .load(data).columns
    logger.info(f"Schema for the {data} is {data_schema}")
    logger.info(f"Mandatory columns schema is {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f"missing columns are {missing_columns}")

    if missing_columns:
        error_files.append(data)
    else:
        logger.info(f"No missing columns for the {data}")
        correct_files.append(data)

logger.info(f"************* list of correct files ************ {correct_files}")
logger.info(f"************* List of incorrect files ********** {error_files}")
logger.info(f"************* Moving error data to error directory if any ****")

#Move the data to error directory on local

error_folder_local_path = config.error_folder_path_local

if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name =os.path.basename(file_path)
            destination_path = os.path.join(error_folder_local_path ,file_name)

            shutil.move(file_path ,destination_path)
            logger.info(f"Moved {file_path} from s3 to file path to {destination_path}")

            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory

            message = move_s3_to_s3(s3_client, config.bucket_name,source_prefix,destination_prefix,file_name)
            logger.info(f"{message}")
        else:
            logger.error(f"'{file_path}' does not exist.")
else:
    logger.info(f"**********There is no error files available at our dataset *****")

#Additional columns need to be taken care off
#Determine extra columns

#Before running the process
#staging table need to be updated with status as Active(A) or inactive(I)

logger.info(f"************* update the product_staging_table that we have started the process ****")
insert_statements = []
db_name = config.database_name
current_date = datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statement = f"""INSERT INTO {db_name}.{config.product_staging_table}
                    (file_name,file_location,created_date,status) VALUES ('{filename}','{filename}','{formatted_date}','A')"""
        insert_statements.append(statement)
    logger.info(f"Insert statement created for staging table --{insert_statements}")
    logger.info("******* Connecting to my sql server  ***")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("******* MY SQL server connected successfully ***")
    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error("***** There is no files to process ****")
    raise Exception("**** No data available with correct files ****")

logger.info("******* Staging table updated successfully *****")
logger.info("******* Fixing extra columns coming form source **")



schema = StructType([
    StructField("customer_id" , IntegerType() , True),
    StructField("store_id" , IntegerType() , True),
    StructField("product_name" , StringType() , True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id",IntegerType(),True),
    StructField("price",FloatType(),True),
    StructField("quantity",IntegerType(),True),
    StructField("total_cost",FloatType(),True),
    StructField("additional_column",StringType(),True)
])

#connecting database Reader
database_client = DatabaseReader(config.url,config.properties)
logger.info("************* creating empty dataframe *******")
#final_df_to_process = database_client.create_dataframe(spark,"empty_df_create_table")

final_df_to_process = spark.createDataFrame([],schema=schema)
#we are using 211 line method to create empty table in DB as 213 was creating error so either use 211 or 213
# Create a new column with concatenate values of extra columns

for data in correct_files:
    data_df = spark.read.format("csv")\
        .option("header","true")\
        .option("inferSchema","true")\
        .load(data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info(f"Extra columns present at source is {extra_columns}")
    if extra_columns:
        data_df = data_df.withColumn("additional_column",concat_ws(", ",*extra_columns))\
            .select("customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost","additional_column")
        logger.info(f"processed {data} and added in 'additional_column'")

    else:
        data_df = data_df.withColumn("additional_column",lit(None)) \
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity",
                    "total_cost", "additional_column")
    data_df.show()
    final_df_to_process.show()
    final_df_to_process = final_df_to_process.union(data_df)
#final_df_to_process = data_df
logger.info("***** Final dataframe from source which will be going to process ****")
final_df_to_process.show()
#final_df_to_process.filter(final_df_to_process["additional_column"].isNotNull()).show()

#Enter the dat from all dimension table
#Also create a datamart for sales team and their incentive address and all
#another datamart for customer who brought how many each days of month
#for every month there should be a file and inside that there sgould be a store_id segrigatin
#Read the data from parqet and generate a csv file in which there willbe a sales_person_name, sales_person_store_id
#sales_person_total_billing_done_for_each_month , total_incentive

#connectin with DatabasesReader

database_client = DatabaseReader(config.url,config.properties)
#create df for all tables
#customer table
logger.info("********* Loading customer table into customer_table_df *********")
customer_table_df = database_client.create_dataframe(spark,config.customer_table_name)
customer_table_df.show()
#product table
logger.info("********* Loading product table into product_table_df *********")
product_table_df = database_client.create_dataframe(spark,config.product_table)
product_table_df.show()
#product staging table
logger.info("********* Loading staging table into staging_table_df *********")
product_staging_table_df = database_client.create_dataframe(spark,config.product_staging_table)
product_staging_table_df.show()
#sales_team_table
logger.info("********* Loading sales team table into sales_team_table_df *********")
sales_team_table_df = database_client.create_dataframe(spark,config.sales_team_table)
sales_team_table_df.show()
#store table
logger.info("********* Loading store table into store_table_df *********")
store_table_df = database_client.create_dataframe(spark,config.store_table)
store_table_df.show()
s3_customer_store_sales_df_join = dimesions_table_join(final_df_to_process,
                                                       customer_table_df,store_table_df,sales_team_table_df)
#final enriched data
logger.info("********* Final enriched data *********")
s3_customer_store_sales_df_join.show()


#Write the customer data into customer data mart in parqut format
#file will be written to local first then move the raw data to s3 bucket for reporting tool
#write reporting data into mysql table also
logger.info("************* write the data into Customer data mart *****")
final_customer_data_mart_df = s3_customer_store_sales_df_join\
    .withColumn("sales_date_month",substring(s3_customer_store_sales_df_join["sales_date"],1,7))\
    .withColumn("sales_month",date_format(s3_customer_store_sales_df_join["sales_date"],"MM"))\
    .withColumn("sales_year",date_format(s3_customer_store_sales_df_join["sales_date"],"yyyy"))\
    .select("customer_id","first_name","last_name","ct.address","pincode",
            "phone_number","sales_date","total_cost","sales_date_month","sales_month","sales_year")
logger.info("************ Final Data for Customer data mart ********")
final_customer_data_mart_df.show()

parquet_writer = GeneralWriter("overwrite","parquet")
parquet_writer.dataframe_writer(final_customer_data_mart_df,config.customer_data_mart_local_file)

logger.info(f"**** Customer data written to local disk at {config.customer_data_mart_local_file}")

#Move data on s3 bucket for customer_ata_mart
logger.info(f"*********** Data movement from local to s3 for customer ******")
s3_uploader = UploadToS3(s3_client)
s3_directory = config.s3_customer_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory,config.bucket_name,config.customer_data_mart_local_file)
logger.info(f"{message}")

#sales team datamart
logger.info("************** writing data into sales team data mart *****")
final_sales_team_data_mart_df = s3_customer_store_sales_df_join \
    .withColumn("sales_month", date_format(s3_customer_store_sales_df_join["sales_date"], "MM")) \
    .withColumn("sales_year", date_format(s3_customer_store_sales_df_join["sales_date"], "yyyy")) \
    .select("store_id","sales_person_id","sales_person_first_name","sales_person_last_name","store_manager_name",
            "manager_id","is_manager","sales_person_address","sales_person_pincode",
            "sales_date","total_cost",
            "sales_month","sales_year")
logger.info("**** Final data for sales team data mart ******")
final_sales_team_data_mart_df.show()
parquet_writer.dataframe_writer(final_sales_team_data_mart_df,config.sales_team_data_mart_local_file)
logger.info(f"*** sales team data written to local disk at {config.sales_team_data_mart_local_file}")

#move data on s3 bucket for sales_data_mart
s3_directory = config.s3_sales_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory,config.bucket_name,config.sales_team_data_mart_local_file)
logger.info(f"{message}")

#Also writing the data into partition
final_sales_team_data_mart_df.write.format("parquet")\
    .option("header","true")\
    .mode("overwrite")\
    .partitionBy("sales_month","store_id")\
    .option("path",config.sales_team_data_mart_partitioned_local_file)\
    .save()
#move data on s3 for partition folder
s3_prefix = "sales_partition_data_mart"
current_epoch = int(datetime.now().timestamp())*1000
for root,dirs,files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        print(file)
        local_file_path = os.path.join(root,file)
        relative_file_path = os.path.relpath(local_file_path,config.sales_team_data_mart_partitioned_local_file)
        s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path}"

#calculation for customer mart
#find out the customer total purchase every month
#write the data into mysql table
logger.info("***** Calculating customer every month purchased amount ***")
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info("******* Calculation of customer mark dne and written into the table ****")

#calculation fir sales team mart
#find out the total sales done by each salesperson every month
#give the top performer 1% incentive of total sales of the month rest sales person will get nothing
#write the data into mysql table
logger.info("***** Calculating the sales every month billing amount ***********")
sales_mart_calculation_table_write(final_sales_team_data_mart_df)
logger.info("*** Calculation of sales mart done and written into the table ***")

#######last step **************
#move the file on s3 into processed folder and delete the local files
source_prefix = config.s3_source_directory
destination_prefix = config.s3_processed_directory
message = move_s3_to_s3(s3_client,config.bucket_name,source_prefix,destination_prefix)
logger.info(f"{message}")

logger.info("********** deleting sales raw file data form local **************")
delete_local_file(config.local_directory)
logger.info("********** deleted sales raw file data form local **************")

logger.info("********** deleting sales team data form local **************")
delete_local_file(config.sales_team_data_mart_local_file)
logger.info("********** deleted sales data team form local **************")

logger.info("********** deleting customer data form local **************")
delete_local_file(config.customer_data_mart_local_file)
logger.info("********** deleted customer data form local **************")

logger.info("********** deleting sales team data mart(partition) form local **************")
delete_local_file(config.sales_team_data_mart_partitioned_local_file)
logger.info("********** deleted sales team data mart(partition) form local **************")

#update the statis of staging table
update_statement = []
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statement = f"""UPDATE {db_name}.{config.product_staging_table} SET status = "I", updated_date = '{formatted_date}' WHERE file_name = '{filename}' """
        update_statement.append(statement)
    logger.info(f"******* Update statement created for staging table -- {update_statement}")
    logger.info("*************** Connecting with my sql server ***********************")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("************ My sql server connected successfully ****************")
    for statement in update_statement:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
    logger.info("************ My sql server closed successfully ****************")
else:
    logger.error("*********** There is some error in process in between **************")
    sys.exit()


input("Press enter to terminate")
