#%%
import os
import pandas as pd
import datetime
from models import get_engine 

#%%
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


def read_datafile(path):
    if path.split('.')[-1] == 'csv':
        return pd.read_csv(path, index_col=0)
    elif path.split('.')[-1] == 'db':
        database_url  = f"sqlite:///{path}"
        table_name = path[:-3]
        engine = get_engine(database_url)
        return pd.read_sql_table(table_name, engine)


def curate_data_by_ticker(ingestion_path: str, ticker: str, curation_path: str) -> bool:
        
    curated_data_paths = get_files_from_layer(layer_path=curation_path, ticker=ticker)
    if curated_data_paths:
        curated_df = read_datafile(curated_data_paths[-1])
        # curated_df = pd.read_csv(curated_data_paths[-1], index_col=0)
    else:
        curated_df = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits'])

    ingested_data_paths = get_files_from_layer(layer_path=ingestion_path, ticker=ticker)
    if ingested_data_paths:
        for path in ingested_data_paths:
                df_aux = pd.read_csv(path)
                curated_df = df_aux.set_index(['Date'])\
                                   .combine_first(curated_df.set_index(['Date']))\
                                   .reset_index()        
    else:
        raise TickerNotFoundOnIngestedDataException(ticker)

    return curated_df


def save_curated_data(curation_path: str, ticker: str, curated_data: pd.DataFrame, output_format: str):
    BASE_DIR = os.getcwd()
    curation_path = f"{BASE_DIR}/{curation_path}/{ticker}"
    curation_name = f"{datetime.datetime.now()}"
    if not os.path.exists(curation_path): 
        os.mkdir(curation_path) 
    if output_format == 'csv':
        curated_data.to_csv(f"{curation_path}/{curation_name}.csv")
    if output_format == 'sqlite':
        database_url = f"sqlite:////{curation_path}/{curation_name}.db"
        engine = get_engine(database_url)
        curated_data.to_sql(f"{curation_path}/{curation_name}",
                            con=engine,
                            if_exists='replace')


def curate_data_all_tickers(ingestion_path: str, curation_path: str, output_format: str) -> bool:
    tickers_in_layer = os.listdir(ingestion_path)
    for ticker in tickers_in_layer:
        curated_df = curate_data_by_ticker(ingestion_path=ingestion_path, ticker=ticker, curation_path=curation_path)
        save_curated_data(curation_path=curation_path, ticker=ticker, curated_data=curated_df, output_format=output_format)

    


if __name__=="__main__":
    print('\n')
    # curate_data_by_ticker(ingestion_path="ingested_data", ticker="sopa", curation_path="curated_data")
    curate_data_all_tickers(ingestion_path="ingested_data", curation_path="curated_data", output_format='sqlite')

#%%

# csv_data1 = pd.read_csv("/home/user/PythonProj/yfinance_ingestor/ingested_data/sopa/2022-03-01 17:04:24.566132.csv")
# csv_data2 = pd.read_csv("/home/user/PythonProj/yfinance_ingestor/ingested_data/sopa/2022-03-01 19:18:23.959880.csv")


#%%
# csv_updated = csv_data2.set_index(['Date'])\
#                        .combine_first(csv_data1.set_index(['Date']))\
#                        .reset_index()
