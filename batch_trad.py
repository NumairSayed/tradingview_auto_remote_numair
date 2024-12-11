from tvDatafeed import TvDatafeed, Interval
import pandas as pd
from datetime import datetime
from tqdm.auto import tqdm
from multiprocessing import Pool
import time

# Define the TvDatafeed instance
tv = TvDatafeed()

# Function to log network stats
def log_network_stats(url, response_time):
    with open("network_stats.txt", mode='a') as file:
        file.write(f"URL: {url}, Response Time: {response_time} seconds, Timestamp: {datetime.now()}\n")

def get_data_batch(batch):
    batch_data = []
    for row in batch:
        exchange, sym, alpha, shares = row['exc'], row['bse_s'], row['AC'], row['shares']
        try:
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
            batch_data.append(filtered_df)

        except Exception as e:
            with open("log_bse_t.txt", mode='a') as file:
                file.write(f"{alpha},,{sym},{e},{datetime.now()}\n")
    return pd.concat(batch_data) if batch_data else None

def process_chunk(chunk):
    batch_size = 10  # Number of rows to process in each batch
    chunk = chunk.to_dict('records')
    for i in range(0, len(chunk), batch_size):
        batch = chunk[i:i + batch_size]
        batch_df = get_data_batch(batch)
        if batch_df is not None:
            batch_df.to_csv("data.csv", mode='a', index=False)

def bse():
    df = pd.read_excel("./tv_final.xlsx")

    # Split the DataFrame into 8 roughly equal chunks
    chunks = [df.iloc[i::16] for i in range(16)]

    start_time = datetime.now()

    # Use multiprocessing to process chunks in parallel
    with Pool(processes=16) as pool:
        list(tqdm(pool.imap(process_chunk, chunks), total=len(chunks), desc="Processing chunks"))

    print(f"Total time: {datetime.now() - start_time}")

if __name__ == '__main__':
    bse()
