#%% 
from abc import ABC, abstractmethod
import time
from typing import List
import datetime
import os.path
import json
import yfinance as yf
import pandas as pd
 
from writers import DataWriter, S3Writer
 
import logging
import sys

from dotenv import load_dotenv
import boto3 

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt='%m/%d/%Y %H:%M:%S',
    handlers=[
        logging.FileHandler("logging.log"),
        logging.StreamHandler(sys.stdout)
    ])
 

class DataIngestor(ABC):

    def __init__(self, writer, ticker: list, batch_ingest: bool, interval: str, period: str=None, ticker_group: str="Undefined", start: datetime=None, end: datetime=None, prepost: bool=False, auto_adjust: bool=True, actions: bool=True, default_start_time: datetime=datetime.date(1800, 1, 1)) -> None:
        self.default_start_date = None
        self.ticker = ticker
        self.ticker_group = ticker_group
        self.period = period
        self.interval = interval
        self.start = start
        self.end = end
        self.prepost = prepost
        self.auto_adjust = auto_adjust
        self.actions = actions
        self.writer = writer
        self.batch_ingest = batch_ingest
        self._checkpoint = self._load_checkpoint()
 
    @property
    def _checkpoint_filename(self) -> str:
        return f"checkpoints/{self.__class__.__name__}.checkpoint"
 
    def _write_checkpoint(self) -> None:
        if not os.path.exists("./checkpoints"):
            os.mkdir("./checkpoints")
        with open(self._checkpoint_filename, "w") as f:
            json.dump(self._checkpoint, f)
 
    def _load_checkpoint(self) -> dict:
        try:
            with open(self._checkpoint_filename, "r") as f:
                checkpoint = json.load(f)
            same_ingestion_flag = self._check_if_ingesting_new_ticker(checkpoint)
            if same_ingestion_flag is False:
                checkpoint = self._append_checkpoint(checkpoint)
            return checkpoint
        except FileNotFoundError:
            return {tick: None for tick in self.ticker}
 
    def _check_if_ingesting_new_ticker(self, checkpoint) -> bool:
        ticker_to_be_ingested = sorted(self.ticker)
        ticker_already_ingested = sorted(list(checkpoint.keys()))
        same_ingestion_flag = ticker_already_ingested == ticker_to_be_ingested
        return same_ingestion_flag
 
    def _append_checkpoint(self, checkpoint) -> dict:
        outer_join_keys = [k for k in self.ticker if k not in list(checkpoint.keys())]
        outer_join_dict = {tick: None for tick in outer_join_keys}
        new_checkpoint = {**checkpoint, **outer_join_dict}
        return new_checkpoint

    def _update_checkpoint(self, key, value) -> None:
        self._checkpoint[str(key)] = value
        self._write_checkpoint()

    def _verify_ingestion_conditions(self, key) -> bool:
        ingestion_flag = True
        checkpoint_date = self._checkpoint[str(key)]
        current_date = datetime.date.today()
        if checkpoint_date is not None:
            checkpoint_date = datetime.datetime.strptime(checkpoint_date, "%Y-%m-%d").date()
            if current_date < checkpoint_date:
                ingestion_flag = False

        return ingestion_flag

    @abstractmethod
    def ingest(self) -> None:
        pass


class TickerDataIngestor(DataIngestor):

    def get_data(self, api, ticker, latest_date, current_date):
        try:
            if latest_date is None:
                data_df = self.get_entire_history_data(api, ticker)
            elif latest_date <= current_date:
                latest_date = datetime.datetime.strptime(latest_date, "%Y-%m-%d").date()
                data_df = self.get_data_starting_from_date(api, latest_date, ticker)
            logger.info(f"Ingesting ticker={ticker}. Num of entries={data_df.shape[0]}, Latest date={latest_date}, Current date={current_date}")
            time.sleep(3)
        except Exception as e:
            print(e)
            data_df = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits', "Ticker"])
            logger.info(f"An exception occured while ingesting '{ticker}': {e}")
        
        return data_df

    def ingest(self) -> None:
        batched_data, batched_checkpoint_keys_values = [], []
        for ticker in self.ticker:
            ingestion_flag = self._verify_ingestion_conditions(ticker)
            if ingestion_flag:
                api = yf.Ticker(ticker)
                latest_date = self._checkpoint[ticker]
                current_date = datetime.date.today().strftime("%Y-%m-%d")
                data_df = self.get_data(api, ticker, latest_date, current_date)

                if self.batch_ingest is not True:
                    self._update_checkpoint(ticker, current_date)
                    self.writer.write(data_df, ticker, self.ticker_group)
                else:
                    batched_data.append(data_df)
                    batched_checkpoint_keys_values.append((ticker, current_date))
            else:
                logger.info(f"The ingestion conditions for ticker '{ticker}' were not fulfilled.")

        if self.batch_ingest:
            self.writer.batch_write(batched_data, "batch", self.ticker_group)
            for k, v in batched_checkpoint_keys_values:
                self._update_checkpoint(k, v)

    def get_data_starting_from_date(self, api, start, ticker) -> pd.DataFrame:
        data_df = api.history(start=start, interval=self.interval, prepost=self.prepost, actions=self.actions, auto_adjust=self.auto_adjust)
        data_df['Ticker'] = ticker.upper()
        return data_df

    def get_entire_history_data(self, api, ticker) -> pd.DataFrame:
        data_df = api.history(period="max", interval=self.interval, prepost=self.prepost, actions=self.actions, auto_adjust=self.auto_adjust)
        data_df['Ticker'] = ticker.upper()
        return data_df


if __name__=="__main__":

    load_dotenv('/home/user/.env')
    # AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    # AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
    bucket_prefix = "dms"
    
    writer = S3Writer(client=s3_client, bucket_prefix=bucket_prefix)
    
    # writer = DataWriter()

    my_ticker = ["AAPL"] #, "GOOG"
    # my_ticker = ["MGLU3.SA", "WEGE3.SA"]
    ticker_group = "NASDAQ" # ticker_prefix

    ticker_ingestor = TickerDataIngestor(
        writer=writer,
        ticker=my_ticker,
        ticker_group=ticker_group,
        interval="1d",
        batch_ingest=False,
    )

    ticker_ingestor.ingest()
    