from abc import abstractmethod 
import datetime 
from io import UnsupportedOperation 
import json 
import os 
from typing import List 
import pandas as pd 
from requests.api import head 
from yfinance import ticker 
# from models import Ticker1DayModel, create_table, get_engine 
from sqlalchemy.orm import sessionmaker 
import pandas as pd 
 
class DataTypeNotSupportedForIngestionException(Exception): 
    def __init__(self, data): 
        self.data = data 
        self.message = f"Data type {type(data)} is not supported for ingestion" 
        super().__init__(self.message) 
 
 
class DataWriter: 
 
    def __init__(self, ticker: str) -> None: #, api: str 
        self.ticker = ticker 
        # self.api = api 
        # self.filename = f"{self.api}/{self.ticker}/{datetime.datetime.now()}.csv" 
        self.filename = f"tickers/{self.ticker}/{datetime.datetime.now()}.csv" 
 
    # def _write_row(self, row: str) -> None: 
    #     os.makedirs(os.path.dirname(self.filename), exist_ok=True) 
    #     with open(self.filename, "a") as f: 
    #         f.write(row) 
 
    # def write(self, data): 
    #     if isinstance(data, pd.DataFrame): 
    #         self._write_row(json.dumps(data) + "\n") 
    #     elif isinstance(data, List): 
    #         for element in data: 
    #             self.write(element) 
    #     else: 
    #         raise DataTypeNotSupportedForIngestionException(data) 
 
    def write(self, data: pd.DataFrame): 
        if isinstance(data, pd.DataFrame): 
            os.makedirs(os.path.dirname(self.filename), exist_ok=True) 
            data.to_csv(self.filename) 
            # self._write_row(json.dumps(data) + "\n") 
        else: 
            raise DataTypeNotSupportedForIngestionException(data) 
 
 
# class ORMWriterOld: 
 
#     def __init__(self, ticker: str, database_url: str) -> None: 
#         """ 
#         Initializes database connection and sessionmaker 
#         Creates tables 
#         """ 
#         self.ticker = ticker 
#         self.database_url = database_url 
#         engine = get_engine(self.create_database_url) 
#         create_table(engine) 
#         self.factory = sessionmaker(bind=engine) 
 
#     @abstractmethod 
#     def create_database_url(self): 
#         pass 
 
#     def convert_df_to_db_entries(self, df): 
#         new_entries = [] 
#         df.reset_index(inplace=True) 
#         for i in range(df.shape[0]): 
#             row = df.iloc[i] 
#             model = Ticker1DayModel() 
#             model.symbol       = self.ticker 
#             model.date         = row["Date"] 
#             model.open         = float(row["Open"]) 
#             model.high         = float(row["High"]) 
#             model.low          = float(row["Low"]) 
#             model.close        = float(row["Close"]) 
#             model.volume       = int(row["Volume"]) 
#             model.dividends    = float(row["Dividends"]) 
#             model.stock_splits = float(row["Stock Splits"]) 
#             new_entries.append(model) 
#         return new_entries 
 
#     def _write(self, df:pd.DataFrame): 
#         pass 
 
#     def write(self, df:pd.DataFrame): 
#         new_entries = self.convert_df_to_db_entries(df) 
#         session = self.factory() 
#         try: 
#             session.bulk_save_objects(new_entries) 
#             session.commit() 
#         except: 
#             print('rollback') 
#             session.rollback() 
#             raise 
#         finally: 
#             session.close() 
 
#     def update_or_add(self, entry): 
#         session = self.factory() 
#         duplicate_data = (session 
#             .query(Ticker1DayModel) 
#             .filter_by(date=entry.date, symbol=entry.symbol) 
#             .first()) 
#         try: 
#             if duplicate_data is not None: 
#                 duplicate_data = entry 
#             else: 
#                 session.add(entry) 
#             session.commit() 
#         except: 
#             print('rollback') 
#             session.rollback() 
#             raise 
#         finally: 
#             session.close() 
 
#     def append(self, df:pd.DataFrame): 
#         new_entries = self.convert_df_to_db_entries(df) 
#         session = self.factory() 
#         for entry in new_entries: 
#             self.update_or_add(entry)     
 
 
# class PostgresWriter(ORMWriterOld): 
 
#     def __init__(self, ticker: str, user, passwd, host, port, db) -> None: 
#         self.user = user 
#         self.passwd = passwd 
#         self.host = host 
#         self.port = port 
#         self.db = db 
#         super().__init__(ticker=ticker, data_storage_url=self.create_database_url) 
 
#     @property 
#     def create_database_url(self): 
#         return f"postgresql://{self.user}:{self.passwd}@{self.host}:{self.port}/{self.db}" 
 
 
# class SQLiteWriter(ORMWriterOld): 
 
#     def __init__(self, ticker: str, db_directory: str, db_name: str) -> None: 
#         self.db_directory = db_directory 
#         self.db_name = db_name 
#         super().__init__(ticker=ticker, database_url=self.create_database_url) 
 
#     @property 
#     def create_database_url(self): 
#         return f"sqlite:////{self.db_directory}/{self.db_name}.db" 
 
 
class CsvWriter(DataWriter): 
 
    @property 
    def csv_filename(self): 
        return f"{self.data_storage_url}/{self.ticker}.csv" 
 
    def write(self, df: pd.DataFrame): 
        df.to_csv(self.csv_filename, mode='w') 
 
    def append(self, df: pd.DataFrame): 
        df.to_csv(self.csv_filename, mode='a', header=False) 
 
 
 
        # self.filename = f"{self.api}/{self.coin}/{datetime.datetime.now()}.json" 
 
    # def _write_row(self, row: str) -> None: 
    #     os.makedirs(os.path.dirname(self.filename), exist_ok=True) 
    #     with open(self.filename, "a") as f: 
    #         f.write(row) 
 
    # def write(self, data: [List, dict]): 
    #     if isinstance(data, dict): 
    #         self._write_row(json.dumps(data) + "\n") 
    #     elif isinstance(data, List): 
    #         for element in data: 
    #             self.write(element) 
    #     else: 
    #         raise DataTypeNotSupportedForIngestionException(data) 