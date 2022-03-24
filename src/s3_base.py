from botocore.exceptions import ClientError
import logging
import os

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class S3BaseClass():

    def __init__(self, client, bucket_name) -> None:
        self.client = client
        self.bucket_name = bucket_name
        self.full_path = ''

    def create_s3_bucket(self):
        """Create an S3 bucket in a specified region
        :return: True if bucket created, else False
        """
        try:
            self.client.create_bucket(Bucket=self.bucket_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def upload_to_s3(self, full_path, ticker, ticker_group):
        """Upload a file to an S3 bucket
        :return: True if file was uploaded, else False
        """
        object_name = f"{ticker_group}/{ticker}/{os.path.basename(full_path)}"
        try:
            response = self.client.upload_file(full_path, self.bucket_name, object_name)
            logger.info(f"Object '{object_name}' was uploaded on bucket '{self.bucket_name}'")
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def get_list_of_existing_buckets(self):
        response = self.client.list_buckets()
        return [bucket["Name"] for bucket in response['Buckets']]

    def check_if_bucket_exists(self):
        buckets = self.get_list_of_existing_buckets()
        return True if self.bucket_name in buckets else False

    def get_object_keys_from_bucket(self, ticker, ticker_group):
        response = self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=f"{ticker_group}/{ticker}/")
        file_count = response['KeyCount']
        if file_count > 0:
            list_of_objects = [content['Key'] for content in response["Contents"]]
            return list_of_objects
        else:
            print(f"Bucket '{self.bucket_name}' is empty.")
            return []