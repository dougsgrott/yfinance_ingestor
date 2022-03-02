#%%
import os
import pandas as pd
import datetime

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


def curate_data_by_ticker(ingestion_path: str, ticker: str, curation_path: str) -> bool:
    
    # get data from curated layer
    # upsert with data from raw layer
    # save to curated layer
    
    curated_data_paths = get_files_from_layer(layer_path="curated_data", ticker=ticker)
    if curated_data_paths:
        current_df = pd.read_csv(curated_data_paths[-1], index_col=0)
    else:
        current_df = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits'])
    print(current_df.shape)

    ingested_data_paths = get_files_from_layer(layer_path="ingested_data", ticker=ticker)
    print(ingested_data_paths)

    if ingested_data_paths:
        for path in ingested_data_paths:
                df_aux = pd.read_csv(path)
                current_df = df_aux.set_index(['Date'])\
                                   .combine_first(current_df.set_index(['Date']))\
                                   .reset_index()
                print(f"{df_aux.shape} {current_df.shape}")
        BASE_DIR = os.getcwd()
        curation_path = f"{BASE_DIR}/{curation_path}/{ticker}/{datetime.datetime.now()}.csv"
        current_df.to_csv(curation_path)
    # else:
    #     raise TickerNotFoundOnIngestedDataException(ticker)


if __name__=="__main__":
    print('\n')
    curate_data_by_ticker(ingestion_path="ingested_data", ticker="sopa", curation_path="curated_data")

#%%

# csv_data1 = pd.read_csv("/home/user/PythonProj/yfinance_ingestor/ingested_data/sopa/2022-03-01 17:04:24.566132.csv")
# csv_data2 = pd.read_csv("/home/user/PythonProj/yfinance_ingestor/ingested_data/sopa/2022-03-01 19:18:23.959880.csv")


#%%
# csv_updated = csv_data2.set_index(['Date'])\
#                        .combine_first(csv_data1.set_index(['Date']))\
#                        .reset_index()
