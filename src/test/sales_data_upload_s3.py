import os
from resources.dev.config import *
from src.main.utility.s3_client_object import *
from src.main.utility.encrypt_decrypt import *
s3_client_provider = S3ClientProvider(decrypt_data(encrypted_aws_access_key), decrypt_data(encrypted_aws_secret_key))
print(s3_client_provider.get_client())
s3_client = s3_client_provider.get_client()

local_file_path = base_location + "sales_data_to_s3"
def upload_to_s3(s3_directory, s3_bucket, local_file_path):
    s3_prefix = f"{s3_directory}"
    try:
        for root, dirs, files in os.walk(local_file_path):
            for file in files:
                print(file)
                local_file_path = os.path.join(root, file)
                s3_key = f"{s3_prefix}{file}"
                s3_client.upload_file(local_file_path, s3_bucket, s3_key)
    except Exception as e:
        raise e

s3_directory = s3_source_directory
s3_bucket = bucket_name
upload_to_s3(s3_directory, s3_bucket, local_file_path)