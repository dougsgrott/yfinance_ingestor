from abc import ABC, abstractmethod
import datetime
import os
import pandas as pd
import logging
from s3_base import S3BaseClass

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class DataTypeNotSupportedForIngestionException(Exception):
    def __init__(self, data):
        self.data = data
        self.message = f"Data type {type(data)} is not supported for ingestion"
        super().__init__(self.message)


#####################################################################
# On-Premise File Writers
#####################################################################

class FileBaseClass(ABC):
    def __init__(self, base_directory, ticker: str='', ticker_group: str='') -> None:
        self.base_directory = base_directory
        self.filename = ''
        self.filepath = ''
        self.full_path = ''

    @abstractmethod
    def _write(self, data: pd.DataFrame):
        pass

    def write(self, data: pd.DataFrame, ticker: str, ticker_group: str):
        self.filepath = f"{self.base_directory}/{ticker_group}/{ticker}/"
        self.filename = f"{datetime.datetime.now()}"
        self.full_path = f"{self.base_directory}/{ticker_group}/{ticker}/{self.filename}"

        os.makedirs(os.path.dirname(self.full_path), exist_ok=True)
        if isinstance(data, pd.DataFrame):
            try:
                self._write(data)
                logger.info(f"Wrote data at {self.full_path}")
            except Exception as e:
                logger.info("Failed to write data due to exception '{e}'")
        else:
            raise DataTypeNotSupportedForIngestionException(data)

    def batch_write(self, batch_data: list, ticker: str, ticker_group: str):
        if isinstance(batch_data, list):
            batch_df = pd.DataFrame()
            for data in batch_data:
                batch_df = pd.concat([batch_df, data], axis=0)
            self.write(batch_df, ticker, ticker_group)


class CsvWriter(FileBaseClass):
    def _write(self, data: pd.DataFrame):
        data.to_csv(f"{self.full_path}.csv")


class ParquetWriter(FileBaseClass):
    def _write(self, data: pd.DataFrame):
        data.to_parquet(f"{self.full_path}.parquet")


#####################################################################
# Cloud Object Writers
#####################################################################

class FileS3Writer(S3BaseClass):
    
    @abstractmethod
    def _write(self, data):
        pass

    def write(self, data: pd.DataFrame, ticker: str, ticker_group: str):
        filename = f"{datetime.datetime.now()}"
        self.full_path = f"s3://{self.bucket_name}/{ticker_group}/{ticker}/{filename}"
        self.create_s3_bucket()
        self._write(data)

    def batch_write(self, batch_data: list, ticker: str, ticker_group: str):
        if isinstance(batch_data, list):
            batch_df = pd.DataFrame()
            for data in batch_data:
                batch_df = pd.concat([batch_df, data], axis=0)
            self.write(batch_df, ticker, ticker_group)

class CsvS3Writer(FileS3Writer):
    def _write(self, data):
        data.to_csv(f"{self.full_path}.csv")


class ParquetS3Writer(FileS3Writer):
    def _write(self, data):
        data.to_parquet(f"{self.full_path}.parquet")


#####################################################################
# On-Premise and Cloud Writer Hybrid
#####################################################################

class CsvS3Transfer(CsvWriter, S3BaseClass):

    def __init__(self, client, bucket_name, base_directory) -> None:
        self.csv_writer = CsvWriter(base_directory)
        self.s3_base_class = S3BaseClass(client=client, bucket_name=bucket_name)

    def write(self, data: pd.DataFrame, ticker: str, ticker_group: str):
        self.csv_writer.write(data, ticker=ticker, ticker_group=ticker_group)
        self.s3_base_class.create_s3_bucket()
        full_path = f"{self.csv_writer.full_path}.csv"
        self.s3_base_class.upload_to_s3(full_path=full_path, ticker=ticker, ticker_group=ticker_group)
