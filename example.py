from crawl import Crawler
import os
import requests
import json
import pandas as pd
import random
import csv

import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow as pa
from datetime import datetime

import numpy as np

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())


if __name__ == "__main__":
    # Lấy 5 token bất kỳ

    numRages = 5
    # page = requests.get(
    #     'https://api-dev.nestquant.com/data/symbols?category=crypto')
    # data = json.loads(page.text)
    # symbols = random.choices(data['symbols'], k=numRages)


    symbols = ['ZIL', 'DASH', 'AAVE', 'STX', 'LTC']
    # Lấy dữ liệu 5 của 5 token
    for symbol in symbols:
        # Crawl dữ liệu từ api
        folder_path = './data/' + symbol
        crawler = Crawler(api_key=os.getenv('API_KEY'))
        crawler.download_historical_data(
            category="crypto", symbol=str(symbol), location=folder_path)

        # Tạo một danh sách chứa đường dẫn đến các tệp Parquet trong thư mục
        parquet_files = [os.path.join(folder_path+'/'+symbol+'USDT', file)
                         for file in os.listdir(folder_path+'/'+symbol+'USDT') if file.endswith('.parquet')]

        # Khởi tạo DataFrame rỗng
        df_combined = pd.DataFrame()

        # Đọc từng tệp Parquet và kết hợp dữ liệu vào DataFrame chung
        for parquet_file in parquet_files:
            pf = pq.read_table(parquet_file)
            df = pf.to_pandas(ignore_metadata=True, timestamp_as_object=True)
            df['OPEN_TIME'] = pd.to_datetime(df["OPEN_TIME"], unit='ms')
            df_combined = pd.concat([df_combined, df])

        # Sort dữ liệu theo trường NUMBER_OF_TRADES
        df_combined = df_combined.sort_values(by='NUMBER_OF_TRADES')

        # Lấy số dòng của dataframe
        num_rows = len(df_combined)

        # Tính số dòng của khoảng
        chunk_size = num_rows // numRages

        # Thêm dòng trắng vào giữa từng khoảng
        for i in range(1, numRages):
            insert_index = i * chunk_size
            blank_row = pd.DataFrame({'------'}, index=[insert_index])
            df_combined = pd.concat([df_combined.iloc[:insert_index], blank_row.transpose(
            ), df_combined.iloc[insert_index:]]).reset_index(drop=True)

        # Lưu DataFrame chứa dữ liệu từ tất cả các tệp Parquet thành tệp CSV
        csv_file = 'combined_data/' + symbol + '.csv'
        df_combined.to_csv(csv_file)

