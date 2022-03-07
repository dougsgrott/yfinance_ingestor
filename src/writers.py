import datetime
import os
import pandas as pd
 
 
class DataTypeNotSupportedForIngestionException(Exception):
    def __init__(self, data):
        self.data = data
        self.message = f"Data type {type(data)} is not supported for ingestion"
        super().__init__(self.message)
 
 
class DataWriter:
 
    def __init__(self, ticker: str) -> None:
        self.ticker = ticker
        self.filename = f"ingested_data/{self.ticker}/{datetime.datetime.now()}.csv"

    def write(self, data: pd.DataFrame):
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        if isinstance(data, pd.DataFrame):
            data.to_csv(self.filename)
        else:
            raise DataTypeNotSupportedForIngestionException(data)

    def batch_write(self, batch_data: pd.DataFrame):
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        if isinstance(batch_data, list):
            batch_df = pd.DataFrame()
            for data in batch_data:
                batch_df = pd.concat([batch_df, data], axis=0)
            batch_df.to_csv(self.filename)
        else:
            raise DataTypeNotSupportedForIngestionException(batch_data)
 