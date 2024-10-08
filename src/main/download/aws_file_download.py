import boto3
import traceback
import os


from src.main.utility.logging_config import *

# def get_object_key(text, keyword):
#     print(f"text -->{text} ,keyword--> {keyword}")
#     start_index = text.find(keyword)
#     after_keyword = ""
#     before_keyword = ""
#     if start_index != -1:
#         # Slice from the start to the keyword
#         before_keyword = text[:start_index]
#         # Slice from the keyword to the end
#         after_keyword = text[start_index + len(keyword) + 1:]
#
#         print("Before Keyword:", before_keyword.strip())  # Output: The quick brown
#         print("After Keyword:", after_keyword.strip())    # Output: jumps over the lazy dog.
#     else:
#         print("Keyword not found.")
#     return  after_keyword.strip()

class S3FileDownloader:
    def __init__(self,s3_client, bucket_name, local_directory):
        self.bucket_name = bucket_name
        self.local_directory = local_directory
        self.s3_client = s3_client

    def download_files(self, list_files):
        logger.info("Running download files for these files %s",list_files)
        for key in list_files:
            file_name = os.path.basename(key)
            object_key = key[1:]
            logger.info("File name %s and the object_key is %s",file_name , object_key)
            download_file_path = os.path.join(self.local_directory, file_name)
            try:
                self.s3_client.download_file(self.bucket_name,object_key,download_file_path)
            except Exception as e:
                error_message = f"Error downloading file '{key}': {str(e)}"
                traceback_message = traceback.format_exc()
                print(error_message)
                print(traceback_message)
                raise e

#below code is to check if the file path (object_key) is valid or not
#I was getting error with the above code as the above code is only taking file name (Drug_clean.csv)
# and not sales_data/Drug_clean.csv and because if this I encountered error
# from botocore.exceptions import ClientError
#
# def check_object_exists(s3_client,b_n, object_key):
#     s3 = s3_client
#     try:
#         s3.head_object(Bucket=b_n, Key=object_key)
#         print(f"Object '{object_key}' exists in bucket '{b_n}'.")
#     except ClientError as e:
#         if e.response['Error']['Code'] == '404':
#             print(f"Object '{object_key}' does not exist in bucket '{b_n}'.")
#         else:
#             print(f"Error occurred: {e}")



