from abc import ABC, abstractmethod 
import pandas as pd
import os 

from models import get_engine
from s3_base import S3BaseClass 
 

#####################################################################
# On-Premise File Readers
#####################################################################

class DataReader(ABC):

    def __init__(self, ingestion_path: str, curation_path: str='', only_newest_ingestion: bool=False) -> None:
        self.ingestion_path = ingestion_path
        self.curation_path = curation_path
        self.only_newest_ingestion = only_newest_ingestion
        # self.ingested_file_type = ingested_file_type
        # self.curated_file_type = curated_file_type
        # self.selected_file_extensions = []
    
    @property
    def use_curated_data(self) -> bool:
        return True if self.curation_path != '' else False

    def get_filepaths_from_layer(self, layer_path, selected_file_extensions, ticker, ticker_group):
        tickers_in_layer = os.listdir(f"{layer_path}/{ticker_group}")
        tickers_full_paths = []
        if ticker in tickers_in_layer:
            file_names = os.listdir(f"{layer_path}/{ticker_group}/{ticker}")
            file_names.sort()
            BASE_DIR = os.getcwd()
            tickers_full_paths = [f"{BASE_DIR}/{layer_path}/{ticker_group}/{ticker}/{name}"
                for name in file_names if name.split('.')[-1] in selected_file_extensions]
        return tickers_full_paths

    @abstractmethod
    def read_curation_files(self) -> None:
        pass

    def read_ingestion_files(self, path_list, read_only_newest: bool=False) -> list[pd.DataFrame]:
        current_df = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits', "Ticker"])

        # filtered_paths = [path for path in path_list if path.split('.')[-1] == 'csv']
        if len(path_list) != 0:
            if read_only_newest:
                current_df = [pd.read_csv(path_list[-1])]
            else:
                current_df = [pd.read_csv(path) for path in path_list]
        return current_df

    def read_ingested_layer(self, ticker: str, ticker_group: str) -> list[pd.DataFrame]:
        ingested_filepaths = self.get_filepaths_from_layer(layer_path=self.ingestion_path, selected_file_extensions=['csv'], ticker=ticker, ticker_group=ticker_group)
        ingested_df_list = self.read_ingestion_files(ingested_filepaths)
        return ingested_df_list

    def read_curated_layer(self, ticker: str, ticker_group: str) -> pd.DataFrame:
        if self.use_curated_data:
            curated_filepaths = self.get_filepaths_from_layer(layer_path=self.curation_path, selected_file_extensions=self.selected_file_extensions, ticker=ticker, ticker_group=ticker_group)
        else:
            curated_filepaths = []
        current_df = self.read_curation_files(curated_filepaths)
        return current_df



class CsvReader(DataReader):

    def __init__(self, ingestion_path: str, curation_path: str = '', only_newest_ingestion: bool = False) -> None:
        super().__init__(ingestion_path, curation_path, only_newest_ingestion)
        self.selected_file_extensions = ['csv']

    def read_curation_files(self, path_list) -> list[pd.DataFrame]:
        current_df = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits', "Ticker"])
        
        if len(path_list) != 0:
            current_df = pd.read_csv(path_list[-1])
        return current_df


class ParquetReader(DataReader):

    def __init__(self, ingestion_path: str, curation_path: str = '', only_newest_ingestion: bool = False) -> None:
        super().__init__(ingestion_path, curation_path, only_newest_ingestion)
        self.selected_file_extensions = ['parquet']
    
    def read_curation_files(self, path_list) -> list[pd.DataFrame]:
        current_df = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits', "Ticker"])
        
        if len(path_list) != 0:
            current_df = pd.read_parquet(path_list[-1])
        return current_df


class SqliteReader(DataReader):

    def __init__(self, ingestion_path: str, curation_path: str = '', only_newest_ingestion: bool = False) -> None:
        super().__init__(ingestion_path, curation_path, only_newest_ingestion)
        self.selected_file_extensions = ['db']
    
    def read_curation_files(self, path_list) -> list[pd.DataFrame]:
        current_df = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits', "Ticker"])
        
        if len(path_list) != 0:
            database_url = f"sqlite:///{path_list[-1]}"
            table_name = path_list[-1][:-3]
            engine = get_engine(database_url)
            current_df = pd.read_sql_table(table_name, engine)
        return current_df


#####################################################################
# Cloud Object Readers
#####################################################################

class FileS3Reader(S3BaseClass):
    
    def __init__(self, client, raw_bucket_name, curated_bucket_name) -> None:
        super().__init__(client, bucket_name='')
        self.raw_bucket_name = raw_bucket_name
        self.curated_bucket_name = curated_bucket_name

    def read_ingested_files(self, all_path_list, read_only_newest: bool=False) -> list[pd.DataFrame]:
        current_df = [pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits', "Ticker"])]
        # filtered_paths = [path for path in path_list if path.split('.')[-1] == 'csv']
        if len(all_path_list) != 0:
            path_list = all_path_list[-1] if read_only_newest else all_path_list
            current_df = [pd.read_csv(f"s3://{self.bucket_name}/{path}") for path in path_list]
        return current_df

    # @abstractmethod
    # def read_curated_files(self, all_path_list, read_only_newest: bool=False) -> list[pd.DataFrame]:
    #     pass

    @abstractmethod
    def _read(self, path) -> list[pd.DataFrame]:
        pass

    def read_curated_files(self, all_path_list, read_only_newest: bool=False) -> list[pd.DataFrame]:
        current_df = [pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits', "Ticker"])]
        filtered_paths = [path for path in all_path_list if path.split('.')[-1] == self.object_extension]
        if len(filtered_paths) != 0:
            path_list = filtered_paths[-1] if read_only_newest else filtered_paths
            current_df = [self._read(f"s3://{self.bucket_name}/{path}") for path in path_list]
        return current_df

    def read_ingested_layer(self, ticker: str, ticker_group: str) -> list[pd.DataFrame]:
        self.bucket_name = self.raw_bucket_name
        ingested_data = [pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits', "Ticker"])]
        bucket_exists = self.check_if_bucket_exists()
        if bucket_exists:
            ingested_object_keys = self.get_object_keys_from_bucket(ticker, ticker_group)
            ingested_data = self.read_ingested_files(ingested_object_keys)
        return ingested_data

    def read_curated_layer(self, ticker: str, ticker_group: str) -> pd.DataFrame:
        self.bucket_name = self.curated_bucket_name
        curated_data = [pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits', "Ticker"])]
        bucket_exists = self.check_if_bucket_exists()
        if bucket_exists:
            curated_object_keys = self.get_object_keys_from_bucket(ticker, ticker_group)
            curated_data = self.read_curated_files(curated_object_keys)
        return curated_data


class CsvS3Reader(FileS3Reader):

    def __init__(self, client, raw_bucket_name, curated_bucket_name) -> None:
        super().__init__(client, raw_bucket_name, curated_bucket_name)
        self.object_extension = 'csv'

    def _read(self, path) -> list[pd.DataFrame]:
        return pd.read_csv(path)


class ParquetS3Reader(FileS3Reader):

    def __init__(self, client, raw_bucket_name, curated_bucket_name) -> None:
        super().__init__(client, raw_bucket_name, curated_bucket_name)
        self.object_extension = 'parquet'

    def _read(self, path) -> list[pd.DataFrame]:
        return pd.read_parquet(path)
        