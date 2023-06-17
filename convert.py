from crawl import Crawler
import os
import requests
import json
import pandas as pd
import random
import csv

import pyarrow.parquet as pq
import pyarrow.compute as pc

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())


if __name__ == "__main__":
    # Lấy 5 token bất kỳ

    page = requests.get(
        'https://api-dev.nestquant.com/data/symbols?category=crypto')
    data = json.loads(page.text)
    symbols = data['symbols']


    for symbol in symbols:
        # Crawl dữ liệu từ api
        folder_path = './data'
        crawler = Crawler(api_key=os.getenv('API_KEY'))
        # Tạo một danh sách chứa đường dẫn đến các tệp Parquet trong thư mục
        parquet_files = [os.path.join(folder_path+'/'+symbol+'USDT', file)
                         for file in os.listdir(folder_path+'/'+symbol+'USDT') if file.endswith('.parquet')]

        # Khởi tạo DataFrame rỗng
        df_combined = pd.DataFrame()

        for parquet_file in parquet_files:
            table = pq.read_table(parquet_file)
            df = table.to_pandas()
            if not df.empty:
                df_combined = pd.concat([df_combined, df])
            


        csv_file = 'combined_data_raw/' + symbol + '.csv'
        df_combined.to_csv(csv_file)