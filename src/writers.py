import datetime
import os
import pandas as pd
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class DataTypeNotSupportedForIngestionException(Exception):
    def __init__(self, data):
        self.data = data
        self.message = f"Data type {type(data)} is not supported for ingestion"
        super().__init__(self.message)


class DataWriter:

    def __init__(self, ticker: str='', ticker_group: str='') -> None:
        self.ticker = ticker
        self.ticker_group = ticker_group
        self.filename = ''

    def write(self, data: pd.DataFrame, ticker: str, ticker_group: str):
        self.ticker = ticker
        self.ticker_group = ticker_group
        self.filename = f"ingested_data/{self.ticker_group}/{self.ticker}/{datetime.datetime.now()}.csv"
        
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        if isinstance(data, pd.DataFrame):
            data.to_csv(self.filename)
            logger.info(f"Wrote data for ticker {self.ticker}")
        else:
            raise DataTypeNotSupportedForIngestionException(data)

    def batch_write(self, batch_data: list, ticker: str, ticker_group: str):
        if isinstance(batch_data, list):
            batch_df = pd.DataFrame()
            for data in batch_data:
                batch_df = pd.concat([batch_df, data], axis=0)
            self.write(batch_df, ticker, ticker_group)


class S3Writer(DataWriter):

    def __init__(self, client, bucket_prefix: str, ticker: str='', ticker_group: str='') -> None:
        super().__init__(ticker=ticker, ticker_group=ticker_group)
        self.client = client
        self.bucket_prefix = bucket_prefix
        
    @property
    def bucket_name(self):
        fmt_bucket_prefix = self.format_string(self.bucket_prefix)
        fmt_ticker_group = self.format_string(self.ticker_group)
        fmt_ticker = self.format_string(self.ticker)
        return f"{fmt_bucket_prefix}-raw-{fmt_ticker_group}-{fmt_ticker}"

    def format_string(self, in_string):
        return in_string.lower().split('.')[0]

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

    def upload_to_s3(self):
        """Upload a file to an S3 bucket
        :return: True if file was uploaded, else False
        """
        object_name = os.path.basename(self.filename)
        try:
            response = self.client.upload_file(self.filename, self.bucket_name, object_name)
            logger.info(f"Object '{object_name}' was uploaded on bucket '{self.bucket_name}'")
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def write(self, data: pd.DataFrame, ticker: str, ticker_group: str):
        super().write(data, ticker, ticker_group)
        self.create_s3_bucket()
        self.upload_to_s3()
