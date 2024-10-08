import boto3


class S3ClientProvider:
    def __init__(self, aws_access_key=None, aws_secret_key=None):
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.session = boto3.Session(
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key
        )
        self.s3_client = self.session.client('s3')

    def get_client(self):
        return self.s3_client



# session = boto3.Session(
#     aws_access_key_id="get ak from config",
#     aws_secret_access_key="get sk from config",
# )
# s3 = session.resource('s3')
#
# print(s3)
# bucket_name = 'de-sales-project-01'
# bucket = s3.Bucket(bucket_name)
#
# # List objects in the bucket
# print("Objects in bucket:")
# for obj in bucket.objects.all():
#     print(obj.key)
