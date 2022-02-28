from abc import abstractmethod 
import datetime 
import json 
import os 
from typing import List 
import csv 
from pandas import read_csv 
from pandas.tseries.offsets import Tick 
# from models import Ticker1DayModel, create_table, get_engine 
from sqlalchemy.orm import sessionmaker 
from sqlalchemy import desc 
 
from sqlalchemy import create_engine 
from sqlalchemy_utils import  create_database, database_exists 
 
class DataReader: 
 
    def __init__(self, ticker: str, data_storage_url: str) -> None: 
        self.ticker = ticker 
        self.data_storage_url = data_storage_url 
 
    @abstractmethod 
    def read_checkpoint(self): 
        pass 
 
 
# class ORMReader: 
 
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
 
#     def read_checkpoint(self): 
#         try: 
#             session = self.factory() 
#             most_recent_entry = session.query(Ticker1DayModel).filter_by(symbol=self.ticker).order_by(desc(Ticker1DayModel.date)).first() 
#             return most_recent_entry.date 
#         except AttributeError: 
#             return None 
 
 
# class PostgresReader(ORMReader): 
 
#     def __init__(self, ticker: str, user, passwd, host, port, db) -> None: 
#         self.user = user 
#         self.passwd = passwd 
#         self.host = host 
#         self.port = port 
#         self.db = db 
#         super().__init__(ticker=ticker, database_url=self.create_database_url) 
 
#     @property 
#     def create_database_url(self): 
#         return f"postgresql://{self.user}:{self.passwd}@{self.host}:{self.port}/{self.db}" 
 
 
# class SQLiteReader(ORMReader): 
 
#     def __init__(self, ticker: str, db_directory: str, db_name: str) -> None: 
#         self.db_directory = db_directory 
#         self.db_name = db_name 
#         super().__init__(ticker=ticker, database_url=self.create_database_url) 
 
#     @property 
#     def create_database_url(self): 
#         return f"sqlite:////{self.db_directory}/{self.db_name}.db" 
 
 
class CsvReader(DataReader): 
 
    def __init__(self, ticker: str, data_storage_url: str) -> None: 
        super().__init__(ticker, data_storage_url) 
        self.csv_data = None 
 
    @property 
    def csv_filename(self): 
        return f"{self.data_storage_url}/{self.ticker}.csv" 
 
    def import_csv(self) -> None: 
        try: 
            data = read_csv(self.csv_filename) 
            return data 
        except FileNotFoundError: 
            return None 
 
    def read_checkpoint(self): 
        self.csv_data =  self.import_csv() 
        if self.csv_data is not None: 
            last_date_str = self.csv_data.tail(1)["Date"].values[0] 
            last_date_time = datetime.datetime.strptime(last_date_str, "%Y-%m-%d").date() 
            return last_date_time 
        else: 
            return None 