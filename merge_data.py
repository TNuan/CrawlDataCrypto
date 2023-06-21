import pandas as pd

# Đọc dữ liệu từ file LABEL_BTC.csv và BTC.csv
df_label = pd.read_csv('./data_BTC/LABEL_BTC.csv')
df_btc = pd.read_csv('./data_BTC/BTC.csv')

df_btc = df_btc.drop(["OPEN_TIME"], axis=1)

# Tạo một dataframe mới để chứa kết quả
df_combined = pd.DataFrame()

# Lặp qua từng dòng trong df_label
for index, row in df_label.iterrows():

  # Lấy lần lượt dữ liệu 12 dòng
  btc_rows = df_btc.iloc[index*12:(index+1)*12]
  # Lọc những giá trị của cột và tạo dataframe theo giá trị đã lấy
  first_open = btc_rows["OPEN"].iloc[0]
  last_close = btc_rows["CLOSE"].iloc[-1]
  highest = btc_rows["HIGH"].max()
  lowest = btc_rows["LOW"].min()
  year_and_month = btc_rows["YEAR_AND_MONTH"].iloc[0]

  open_close_df = pd.DataFrame({"OPEN": [first_open], "CLOSE": [last_close], "HIGH": [highest], "LOW": [lowest], "YEAR_AND_MONTH": [year_and_month]})

  # Xóa các cột đã lấy dữ liệu cần thiết và dồn tổng từng cột
  btc_rows = btc_rows.drop(["OPEN", "CLOSE", "HIGH", "LOW", "YEAR_AND_MONTH"], axis=1)
  sum_btc_rows = btc_rows.sum().to_frame().T
  
  # Dồn dữ liệu thành một dòng duy nhất
  df_btc_combined = pd.concat([open_close_df, sum_btc_rows], axis=1)

  reshaped_data_row = row.values.reshape(1, -1)
  df_row_combined = pd.DataFrame(reshaped_data_row, columns=df_label.columns)

  # Kết hợp dòng từ df_label và 12 dòng từ df_btc thành một dòng mới
  combined_row = pd.concat([df_row_combined, df_btc_combined], axis=1)

  # Thêm dòng mới vào dataframe kết quả
  df_combined = pd.concat([df_combined, combined_row], ignore_index=True, axis=0)

# Ghi kết quả vào file mới hoặc ghi đè lên file LABEL_BTC.csv
df_combined.to_csv('LABEL_BTC_with_BTC.csv', index=False)
