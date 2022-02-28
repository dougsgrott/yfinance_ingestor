#%% 
from abc import ABC, abstractmethod 
import time 
from typing import List 
import datetime 
import os.path 
import json 
import yfinance as yf 
 
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
 
#%% 
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
        # self.reader = reader 
        self.writer = writer 
        self._checkpoint = self._load_checkpoint() 
 
    @property 
    def _checkpoint_filename(self) -> str: 
        return f"checkpoints/{self.__class__.__name__}.checkpoint" 
 
    def _write_checkpoint(self): 
        if not os.path.exists("./checkpoints"): 
            os.mkdir("./checkpoints") 
        with open(self._checkpoint_filename, "w") as f: 
            json.dump(self._checkpoint, f) 
            # f.write(f"{self._checkpoint}") 
 
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
 
    def _check_if_ingesting_new_ticker(self, checkpoint): 
        ticker_to_be_ingested = sorted(self.ticker) 
        ticker_already_ingested = sorted(list(checkpoint.keys())) 
        same_ingestion_flag = ticker_already_ingested == ticker_to_be_ingested 
        return same_ingestion_flag 
 
    def _append_checkpoint(self, checkpoint): 
        outer_join_keys = [k for k in self.ticker if k not in list(checkpoint.keys())] 
        outer_join_dict = {tick: None for tick in outer_join_keys} 
        new_checkpoint = {**checkpoint, **outer_join_dict} 
        return new_checkpoint 
 
    def _update_checkpoint(self, key, value): 
        self._checkpoint[str(key)] = value 
        self._write_checkpoint() 
 
    def _verify_ingestion_conditions(self, key): 
        ingestion_flag = True 
        checkpoint_date = self._checkpoint[str(key)] 
        current_date = datetime.date.today() 
        if checkpoint_date is not None: 
            checkpoint_date = datetime.datetime.strptime(checkpoint_date, "%Y-%m-%d").date() 
            if current_date < checkpoint_date: 
                ingestion_flag = False 
 
        return ingestion_flag 
 
 
    # def _write_checkpoint(self): 
    #     with open(self._checkpoint_filename, "w") as f: 
    #         f.write(f"{self._checkpoint}") 
 
    # def _load_checkpoint(self) -> datetime.date: 
    #     try: 
    #         with open(self._checkpoint_filename, "r") as f: 
    #             return datetime.datetime.strptime(f.read(), "%Y-%m-%d").date() 
    #     except FileNotFoundError: 
    #         return self.default_start_date 
 
    # def _update_checkpoint(self, value): 
    #     self._checkpoint = value 
    #     self._write_checkpoint() 
 
    @abstractmethod 
    def ingest(self) -> None: 
        pass 
 
class TickerDataIngestor(DataIngestor): 
 
    def ingest(self) -> None: 
        for ticker in self.ticker: 
            ingestion_conditions = self._verify_ingestion_conditions(ticker) 
            if ingestion_conditions: 
                api = yf.Ticker(ticker) 
                # latest_date = self._load_checkpoint() 
                latest_date = self._checkpoint[ticker] 
                current_date = datetime.date.today()
                current_date = current_date.strftime("%Y-%m-%d")
                if latest_date is None: 
                    data_df = self.ingest_history(api) 
                    self.writer(ticker=ticker).write(data_df) 
                    checkpoint_value = current_date
                    self._update_checkpoint(ticker, checkpoint_value) 
                    print(f"Adding {data_df.shape[0]} entries") 
                    time.sleep(3) 
                elif latest_date <= current_date: 
                    latest_date = datetime.datetime.strptime(latest_date, "%Y-%m-%d").date() 
                    # latest_date += datetime.timedelta(days=1) 
                    data_df = self.ingest_from_date(api, latest_date) 
                    # self.writer.append(data_df) 
                    self.writer(ticker=ticker).write(data_df) 
                    checkpoint_value = current_date
                    self._update_checkpoint(ticker, checkpoint_value) 
                    print(f"Appending {data_df.shape[0]} entries, using date = {latest_date}") 
                    time.sleep(3) 
                logger.info(f"Ingestion progress: Ticker={ticker}, Value={checkpoint_value}, Ingested={ingestion_conditions}") 
                # self._update_checkpoint(latest_date + datetime) 
 
    def ingest_from_date(self, api, start): 
        data_df = api.history(start=start, interval=self.interval, prepost=self.prepost, actions=self.actions, auto_adjust=self.auto_adjust) 
        return data_df 
 
    def ingest_history(self, api): # -> None 
        data_df = api.history(period="max", interval=self.interval, prepost=self.prepost, actions=self.actions, auto_adjust=self.auto_adjust) 
        return data_df 
 
#%% 
 
if __name__=="__main__": 
 
    my_ticker = ["aapl", "sopa"] 
 
    ticker_ingestor = TickerDataIngestor( 
        writer=DataWriter, 
        ticker=my_ticker, 
        # period="1mo", 
        interval="1d" 
    ) 
 
    ticker_ingestor.ingest() 
