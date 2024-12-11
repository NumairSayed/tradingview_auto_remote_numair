from tvDatafeed import TvDatafeed, Interval
import pandas as pd
from datetime import datetime
from tqdm.auto import tqdm
from multiprocessing import Pool
import requests
import time

# Define the TvDatafeed instance
tv = TvDatafeed()

# Function to log network stats
def log_network_stats(url, response_time):
    with open("network_stats.txt", mode='a') as file:
        file.write(f"URL: {url}, Response Time: {response_time} seconds, Timestamp: {datetime.now()}\n")

def get_data(exchange, sym, alpha, shares):
    start_time = time.time()
    nifty_index_data = tv.get_hist(symbol=sym, exchange=exchange, interval=Interval.in_1_minute, n_bars=1)
    response_time = time.time() - start_time

    # Log network stats for hitting the API
    log_network_stats(f"{exchange}/{sym}", response_time)

    nifty_index_data = nifty_index_data.reset_index()
    nifty_index_data['alpha_code'] = alpha
    nifty_index_data['market_cap'] = nifty_index_data['open'] * shares
    nifty_index_data['datetime'] = pd.to_datetime(nifty_index_data['datetime'])
    today_date = datetime.today()
    formatted_date = today_date.strftime('%m/%d/%Y')
    
    # Filter rows where the date matches the current date
    filtered_df = nifty_index_data[nifty_index_data['datetime'].dt.date == pd.to_datetime(formatted_date).date()]
    
    # Append data to CSV
    filtered_df.to_csv("data.csv", mode='a', index=False)

def process_chunk(chunk):
    for i, row in tqdm(chunk.iterrows(), total=chunk.shape[0]):
        comp = row['AC']
        sym = row['bse_s']
        exc = row['exc']
        shares = row['shares']
        try:
            get_data(exchange=exc, sym=sym, alpha=comp, shares=shares)
        except Exception as e:
            with open("log_bse_t.txt", mode='a') as file:
                file.write(f'{comp},,{sym},{e},{datetime.now()}\n')

def bse():
    df = pd.read_excel("./tv_final.xlsx")

    # Split the DataFrame into 4 roughly equal chunks
    chunks = [df.iloc[i::8] for i in range(8)]

    start_time = datetime.now()

    # Use multiprocessing to process chunks in parallel
    with Pool(processes=8) as pool:
        pool.map(process_chunk, chunks)

    print(f'Total time: {datetime.now() - start_time}')

if __name__ == '__main__':
    bse()
