#%% 
from abc import ABC, abstractmethod
import time
from typing import List
import datetime
import os.path
import json
import yfinance as yf
import pandas as pd
 
from writers import DataWriter
 
import logging
import sys
 
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

    def __init__(self, writer, ticker: List, interval: str, period: str=None, start: datetime=None, end: datetime=None, prepost: bool=False, auto_adjust: bool=True, actions: bool=True, default_start_time: datetime=datetime.date(1800, 1, 1)) -> None:
        self.default_start_date = None
        self.ticker = ticker
        self.period = period
        self.interval = interval
        self.start = start
        self.end = end
        self.prepost = prepost
        self.auto_adjust = auto_adjust
        self.actions = actions
        self.writer = writer
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
 
    def ingest(self) -> None:
        for ticker in self.ticker:
            ingestion_conditions = self._verify_ingestion_conditions(ticker)
            if ingestion_conditions:
                api = yf.Ticker(ticker)
                latest_date = self._checkpoint[ticker]
                current_date = datetime.date.today().strftime("%Y-%m-%d")
                
                if latest_date is None:
                    data_df = self.ingest_history(api, ticker)
                elif latest_date <= current_date:
                    latest_date = datetime.datetime.strptime(latest_date, "%Y-%m-%d").date()
                    data_df = self.ingest_from_date(api, latest_date, ticker)

                self.writer(ticker=ticker).write(data_df)
                checkpoint_value = current_date
                self._update_checkpoint(ticker, checkpoint_value)
                logger.info(f"Ingestion progress: Ticker={ticker}, Num of entries={data_df.shape[0]}, Used date={latest_date}, Value={checkpoint_value}, Ingested={ingestion_conditions}")
                time.sleep(3)
 

    def batch_ingest(self) -> None:
        batched_data, batched_checkpoint_keys, batched_checkpoint_values, batched_flag = [], [], [], []
        for ticker in self.ticker:
            ingestion_conditions = self._verify_ingestion_conditions(ticker)
            batched_flag.append(ingestion_conditions)
            if ingestion_conditions:
                try:
                    api = yf.Ticker(ticker)
                    latest_date = self._checkpoint[ticker]
                    current_date = datetime.date.today().strftime("%Y-%m-%d")

                    if latest_date is None:
                        data_df = self.ingest_history(api, ticker)
                    elif latest_date <= current_date:
                        latest_date = datetime.datetime.strptime(latest_date, "%Y-%m-%d").date()
                        data_df = self.ingest_from_date(api, latest_date, ticker)

                    checkpoint_value = current_date
                    batched_data.append(data_df)
                    batched_checkpoint_keys.append(ticker)
                    batched_checkpoint_values.append(checkpoint_value)
                    time.sleep(3)
                except Exception as e:
                    print(e)
            logger.info(f"Batch ingestion progress: Ticker={ticker}, Num of entries={data_df.shape[0]}, Used date={latest_date}, Value={checkpoint_value}, Ingested={ingestion_conditions}")

        if len(batched_data) > 0:
            self.writer(ticker="batch").batch_write(batched_data)
            for k, v, flag in zip(batched_checkpoint_keys, batched_checkpoint_values, batched_flag):
                self._update_checkpoint(k, v)

    def ingest_from_date(self, api, start, ticker) -> pd.DataFrame:
        data_df = api.history(start=start, interval=self.interval, prepost=self.prepost, actions=self.actions, auto_adjust=self.auto_adjust)
        data_df['Ticker'] = ticker.upper()
        return data_df
 
    def ingest_history(self, api, ticker) -> pd.DataFrame:
        data_df = api.history(period="max", interval=self.interval, prepost=self.prepost, actions=self.actions, auto_adjust=self.auto_adjust)
        data_df['Ticker'] = ticker.upper()
        return data_df
 
 
if __name__=="__main__": 
 
    # my_ticker = ["aapl", "sopa"]
    # my_ticker = ["WEGE3.SA"] #, "MGLU3.SA"
    my_ticker = ["MGLU3.SA"]
 
    ticker_ingestor = TickerDataIngestor( 
        writer=DataWriter, 
        ticker=my_ticker, 
        interval="1d",
    ) 
 
    # ticker_ingestor.ingest()
    ticker_ingestor.batch_ingest()