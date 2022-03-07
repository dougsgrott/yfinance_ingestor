import os
import pandas as pd
import datetime
from models import get_engine
import time


class TickerNotFoundOnIngestedDataException(Exception):
    def __init__(self, data: str):
        self.data = data
        self.message = f"Ticker named {data} was not found on ingested data location"
        super().__init__(self.message)


def get_files_from_layer(layer_path: str, ticker: str) -> list[str]:
    tickers_in_layer = os.listdir(layer_path)
    tickers_full_paths = []
    if ticker in tickers_in_layer:
        file_names = os.listdir(f"{layer_path}/{ticker}")
        file_names.sort()
        BASE_DIR = os.getcwd()
        tickers_full_paths = [f"{BASE_DIR}/{layer_path}/{ticker}/{name}" for name in file_names]
    return tickers_full_paths


def read_datafile(path_list, file_format, ticker=None) -> pd.DataFrame:
    curated_df = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits', "Ticker"])
    if file_format == 'csv':
        filtered_paths = [path for path in path_list  if path.split('.')[-1] == 'csv']
        if len(filtered_paths) != 0:
            curated_df = pd.read_csv(filtered_paths[-1])
    
    elif file_format == 'sqlite':
        filtered_paths = [path for path in path_list  if path.split('.')[-1] == 'db']
        if len(filtered_paths) != 0:
            database_url  = f"sqlite:///{filtered_paths[-1]}"
            table_name = filtered_paths[-1][:-3]
            engine = get_engine(database_url)
            curated_df = pd.read_sql_table(table_name, engine)

    elif file_format == 'postgres':
        user = "root"
        passwd = "root"
        host = "localhost"
        port = "5432"
        db = "premise_postgres_db"
        database_url = f"postgresql://{user}:{passwd}@{host}:{port}/{db}"
        engine = get_engine(database_url)
        name = ticker
        curated_df = pd.read_sql_table(name, engine)
    
    elif file_format == 'parquet':
        filtered_paths = [path for path in path_list if "parquet" in path]
        if len(filtered_paths) != 0:
            curated_df = pd.read_parquet(filtered_paths[-1])

    return curated_df


def upsert_dataframe(curated_df, ingested_data_paths):
    for path in ingested_data_paths:
        df_aux = pd.read_csv(path)
        # Merge / Upsert
        curated_df = df_aux.set_index(['Date', 'Ticker'])\
                            .combine_first(curated_df.set_index(['Date', 'Ticker']))\
                            .reset_index()
    return curated_df


def curate_data_by_ticker(ingestion_path: str, ticker: str, curation_path: str, file_format: str) -> bool:
        
    curated_data_paths = get_files_from_layer(layer_path=curation_path, ticker=ticker)
    curated_df = read_datafile(curated_data_paths, file_format, ticker)

    ingested_data_paths = get_files_from_layer(layer_path=ingestion_path, ticker=ticker)
    if ingested_data_paths:
        curated_df = upsert_dataframe(curated_df, ingested_data_paths)
    # else:
    #     raise TickerNotFoundOnIngestedDataException(ticker)

    return curated_df


def save_curated_data(curation_path: str, ticker: str, curated_data: pd.DataFrame, file_format: str, use_datetime_on_output_name: bool):
    BASE_DIR = os.getcwd()
    curation_path = f"{BASE_DIR}/{curation_path}/{ticker}"
    curation_name = f"{datetime.datetime.now()}" if use_datetime_on_output_name else ticker
    if not os.path.exists(curation_path):
        os.mkdir(curation_path)
    if file_format == 'csv':
        curated_data.to_csv(f"{curation_path}/{curation_name}.csv", index=False)
    if file_format == 'sqlite':
        database_url = f"sqlite:////{curation_path}/{curation_name}.db"
        engine = get_engine(database_url)
        name = f"{curation_path}/{curation_name}"
        curated_data.to_sql(name, con=engine, index=False, if_exists='replace')
    if file_format == 'postgres':
        user = "root"
        passwd = "root"
        host = "localhost"
        port = "5432"
        db = "premise_postgres_db"
        database_url = f"postgresql://{user}:{passwd}@{host}:{port}/{db}"
        engine = get_engine(database_url)
        name = ticker
        curated_data.to_sql(ticker, con=engine, index=False, if_exists='replace')
    if file_format == 'parquet':
        name = f"{curation_path}/{curation_name}"
        curated_data.to_parquet(f'{name}.parquet.gzip', compression='gzip')


def curate_data_all_tickers(ingestion_path: str, curation_path: str, file_format: str, use_datetime_on_output_name: bool=False) -> bool:
    tickers_in_layer = os.listdir(ingestion_path)
    for ticker in tickers_in_layer:
        curated_df = curate_data_by_ticker(ingestion_path=ingestion_path, ticker=ticker, curation_path=curation_path, file_format=file_format)
        save_curated_data(curation_path=curation_path, ticker=ticker, curated_data=curated_df, file_format=file_format, use_datetime_on_output_name=use_datetime_on_output_name)


def curate_batch_data(ingestion_path: str, curation_path: str, file_format: str, ingest_latest: bool=False) -> None:
    curated_df = curate_data_by_ticker(ingestion_path=ingestion_path, ticker="batch", curation_path="curated_data", file_format="csv")
    save_curated_data(curation_path=curation_path, ticker="batch", curated_data=curated_df, file_format=file_format, use_datetime_on_output_name=True)


if __name__=="__main__":
    print('\n')
    # curate_data_all_tickers(ingestion_path="ingested_data", curation_path="curated_data", file_format='csv', use_datetime_on_output_name=False)
    curate_batch_data(ingestion_path="ingested_data", curation_path="curated_data", file_format='csv', ingest_latest=False)
    # curate_data_by_ticker(ingestion_path="ingested_data", ticker="batch", curation_path="curated_data", file_format="csv")
    # curate_batch_data(ingestion_path="ingested_data", curation_path="curated_data", file_format="csv", ingest_latest=False)
    # curate_data_all_tickers(ingestion_path="ingested_data", curation_path="curated_data", file_format='parquet', use_datetime_on_output_name=False)
    time.sleep(2)

    # curate_data_all_tickers(ingestion_path="ingested_data", curation_path="curated_data", file_format='parquet', use_datetime_on_output_name=False)
    # time.sleep(2)
    # curate_data_all_tickers(ingestion_path="ingested_data", curation_path="curated_data", file_format='csv', use_datetime_on_output_name=False)
    # time.sleep(2)
    # curate_data_all_tickers(ingestion_path="ingested_data", curation_path="curated_data", file_format='sqlite', use_datetime_on_output_name=False)
    # time.sleep(2)

